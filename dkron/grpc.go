package dkron

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/abronan/valkeyrie/store"
	metrics "github.com/armon/go-metrics"
	pb "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"github.com/victorcoder/dkron/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	ErrExecutionDoneForDeletedJob = errors.New("rpc: Received execution done for a deleted job")
	ErrRPCDialing                 = errors.New("rpc: Error dialing, verify the network connection to the server")
)

type DkronGRPCServer interface {
	proto.DkronServer
	Serve() error
}

type GRPCServer struct {
	agent *Agent
}

// NewRPCServe creates and returns an instance of an RPCServer implementation
func NewGRPCServer(agent *Agent) DkronGRPCServer {
	return &GRPCServer{
		agent: agent,
	}
}

func (grpcs *GRPCServer) Serve() error {
	bindIp, err := grpcs.agent.GetBindIP()
	if err != nil {
		return err
	}
	rpca := fmt.Sprintf("%s:%d", bindIp, grpcs.agent.config.RPCPort)
	log.WithFields(logrus.Fields{
		"rpc_addr": rpca,
	}).Debug("grpc: Registering GRPC server")

	lis, err := net.Listen("tcp", rpca)
	if err != nil {
		log.Fatalf("grpc: failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterDkronServer(grpcServer, grpcs)
	go grpcServer.Serve(lis)

	return nil
}

// Encode is used to encode a Protoc object with type prefix
func Encode(t MessageType, msg interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(uint8(t))
	m, err := pb.Marshal(msg.(pb.Message))
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(m)
	return buf.Bytes(), err
}

// SetJob broadcast a state change to the cluster members that will store the job.
// Then restart the scheduler
// This only works on the leader
func (grpcs *GRPCServer) SetJob(ctx context.Context, setJobReq *proto.SetJobRequest) (*proto.SetJobResponse, error) {
	defer metrics.MeasureSince([]string{"grpc", "set_job"}, time.Now())
	log.WithFields(logrus.Fields{
		"job": setJobReq.Job.Name,
	}).Debug("grpc: Received SetJob")

	cmd, err := Encode(SetJobType, setJobReq.Job)
	if err != nil {
		return nil, err
	}
	af := grpcs.agent.raft.Apply(cmd, raftTimeout)
	if err := af.Error(); err != nil {
		return nil, err
	}

	// If everything is ok, restart the scheduler
	grpcs.agent.SchedulerRestart()

	return &proto.SetJobResponse{}, nil
}

// DeleteJob broadcast a state change to the cluster members that will delete the job.
// Then restart the scheduler
// This only works on the leader
func (grpcs *GRPCServer) DeleteJob(ctx context.Context, delJobReq *proto.DeleteJobRequest) (*proto.DeleteJobResponse, error) {
	defer metrics.MeasureSince([]string{"grpc", "delete_job"}, time.Now())
	log.WithFields(logrus.Fields{
		"job": delJobReq.GetJobName(),
	}).Debug("grpc: Received DeleteJob")

	cmd, err := Encode(DeleteJobType, delJobReq)
	if err != nil {
		return nil, err
	}
	af := grpcs.agent.raft.Apply(cmd, raftTimeout)
	if err := af.Error(); err != nil {
		return nil, err
	}
	res := af.Response()
	job := res.(*Job)
	jpb := job.ToProto()

	return &proto.DeleteJobResponse{Job: jpb}, nil
}

func (grpcs *GRPCServer) GetJob(ctx context.Context, getJobReq *proto.GetJobRequest) (*proto.GetJobResponse, error) {
	defer metrics.MeasureSince([]string{"grpc", "get_job"}, time.Now())
	log.WithFields(logrus.Fields{
		"job": getJobReq.JobName,
	}).Debug("grpc: Received GetJob")

	j, err := grpcs.agent.Store.GetJob(getJobReq.JobName, nil)
	if err != nil {
		return nil, err
	}

	gjr := &proto.GetJobResponse{
		Job: &proto.Job{},
	}

	// Copy the data structure
	gjr.Job.Name = j.Name
	gjr.Job.Executor = j.Executor
	gjr.Job.ExecutorConfig = j.ExecutorConfig

	return gjr, nil
}

func (grpcs *GRPCServer) ExecutionDone(ctx context.Context, execDoneReq *proto.ExecutionDoneRequest) (*proto.ExecutionDoneResponse, error) {
	defer metrics.MeasureSince([]string{"grpc", "execution_done"}, time.Now())
	log.WithFields(logrus.Fields{
		"group": execDoneReq.Group,
		"job":   execDoneReq.JobName,
		"from":  execDoneReq.NodeName,
	}).Debug("grpc: Received execution done")

	var execution Execution
	processed := false

retry:
	// Load the job from the store
	job, jkv, err := grpcs.agent.Store.GetJobWithKVPair(execDoneReq.JobName, &JobOptions{
		ComputeStatus: true,
	})
	if err != nil {
		if err == store.ErrKeyNotFound {
			log.Warning(ErrExecutionDoneForDeletedJob)
			return nil, ErrExecutionDoneForDeletedJob
		}
		log.Fatal("grpc:", err)
		return nil, err
	}

	if !processed {
		// Get the defined output types for the job, and call them
		origExec := *NewExecutionFromProto(execDoneReq)
		execution = origExec
		for k, v := range job.Processors {
			log.WithField("plugin", k).Info("grpc: Processing execution with plugin")
			if processor, ok := grpcs.agent.ProcessorPlugins[k]; ok {
				v["reporting_node"] = grpcs.agent.config.NodeName
				e := processor.Process(&ExecutionProcessorArgs{Execution: origExec, Config: v})
				execution = e
			} else {
				log.WithField("plugin", k).Error("grpc: Specified plugin not found")
			}
		}

		// Save the execution to store
		if _, err := grpcs.agent.Store.SetExecution(&execution); err != nil {
			return nil, err
		}

		processed = true
	}

	if execution.Success {
		job.LastSuccess = execution.FinishedAt
		job.SuccessCount++
	} else {
		job.LastError = execution.FinishedAt
		job.ErrorCount++
	}

	ok, err := grpcs.agent.Store.AtomicJobPut(job, jkv)
	if err != nil && err != store.ErrKeyModified {
		log.WithError(err).Fatal("grpc: Error in atomic job save")
	}
	if !ok {
		log.Debug("grpc: Retrying job update")
		goto retry
	}

	execDoneResp := &proto.ExecutionDoneResponse{
		From:    grpcs.agent.config.NodeName,
		Payload: []byte("saved"),
	}

	// If the execution failed, retry it until retries limit (default: don't retry)
	if !execution.Success && execution.Attempt < job.Retries+1 {
		execution.Attempt++

		// Keep all execution properties intact except the last output
		// as it could exceed serf query limits.
		execution.Output = []byte{}

		log.WithFields(logrus.Fields{
			"attempt":   execution.Attempt,
			"execution": execution,
		}).Debug("grpc: Retrying execution")

		grpcs.agent.RunQuery(&execution)
		return nil, nil
	}

	exg, err := grpcs.agent.Store.GetExecutionGroup(&execution)
	if err != nil {
		log.WithError(err).WithField("group", execution.Group).Error("grpc: Error getting execution group.")
		return nil, err
	}

	// Send notification
	Notification(grpcs.agent.config, &execution, exg, job).Send()

	// Jobs that have dependent jobs are a bit more expensive because we need to call the Status() method for every execution.
	// Check first if there's dependent jobs and then check for the job status to begin execution dependent jobs on success.
	if len(job.DependentJobs) > 0 && job.GetStatus() == StatusSuccess {
		for _, djn := range job.DependentJobs {
			dj, err := grpcs.agent.Store.GetJob(djn, nil)
			if err != nil {
				return nil, err
			}
			log.WithField("job", djn).Debug("grpc: Running dependent job")
			dj.Run()
		}
	}

	return execDoneResp, nil
}

func (grpcs *GRPCServer) Leave(ctx context.Context, in *empty.Empty) (*empty.Empty, error) {
	return in, grpcs.agent.Stop()
}

type DkronGRPCClient interface {
	Connect(string) (*grpc.ClientConn, error)
	CallExecutionDone(string, *Execution) error
	CallGetJob(string, string) (*Job, error)
	CallSetJob(*Job) error
	CallDeleteJob(string) (*Job, error)
	Leave(string) error
}
