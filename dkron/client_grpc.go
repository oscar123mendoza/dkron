package dkron

import (
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"github.com/victorcoder/dkron/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type GRPCClient struct {
	dialOpt []grpc.DialOption
	agent   *Agent
}

func NewGRPCClient(dialOpt grpc.DialOption, agent *Agent) DkronGRPCClient {
	if dialOpt == nil {
		dialOpt = grpc.WithInsecure()
	}
	return &GRPCClient{
		dialOpt: []grpc.DialOption{
			dialOpt,
			grpc.WithBlock(),
			grpc.WithTimeout(5 * time.Second),
		},
		agent: agent,
	}
}

func (grpcc *GRPCClient) Connect(addr string) (*grpc.ClientConn, error) {
	// Initiate a connection with the server
	conn, err := grpc.Dial(addr, grpcc.dialOpt...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (grpcc *GRPCClient) CallExecutionDone(addr string, execution *Execution) error {
	defer metrics.MeasureSince([]string{"grpc", "call_execution_done"}, time.Now())
	var conn *grpc.ClientConn

	conn, err := grpcc.Connect(addr)
	if err != nil {
		log.WithFields(logrus.Fields{
			"err":         err,
			"server_addr": addr,
		}).Error("grpc: error dialing.")
	}
	defer conn.Close()

	d := proto.NewDkronClient(conn)
	edr, err := d.ExecutionDone(context.Background(), &proto.ExecutionDoneRequest{Execution: execution.ToProto()})
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err,
		}).Warning("grpc: Error calling ExecutionDone")
		return err
	}
	log.Debug("grpc: from: ", edr.From)

	return nil
}

func (grpcc *GRPCClient) CallGetJob(addr, jobName string) (*Job, error) {
	defer metrics.MeasureSince([]string{"grpc", "call_get_job"}, time.Now())
	var conn *grpc.ClientConn

	// Initiate a connection with the server
	conn, err := grpcc.Connect(addr)
	if err != nil {
		log.WithFields(logrus.Fields{
			"err":         err,
			"server_addr": addr,
		}).Error("grpc: error dialing.")
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	gjr, err := d.GetJob(context.Background(), &proto.GetJobRequest{JobName: jobName})
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err,
		}).Warning("grpc: Error calling GetJob")
		return nil, err
	}

	return NewJobFromProto(gjr.Job), nil
}

func (grpcc *GRPCClient) Leave(addr string) error {
	var conn *grpc.ClientConn

	// Initiate a connection with the server
	conn, err := grpcc.Connect(addr)
	if err != nil {
		log.WithFields(logrus.Fields{
			"err":         err,
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return err
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	_, err = d.Leave(context.Background(), &empty.Empty{})
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err,
		}).Warning("grpc: Error calling Leave")
		return err
	}

	return nil
}

// CallSetJob calls the leader passing the job
func (grpcc *GRPCClient) CallSetJob(job *Job) error {
	var conn *grpc.ClientConn

	addr := grpcc.agent.raft.Leader()

	// Initiate a connection with the server
	conn, err := grpcc.Connect(string(addr))
	if err != nil {
		log.WithFields(logrus.Fields{
			"err":         err,
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return err
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	_, err = d.SetJob(context.Background(), &proto.SetJobRequest{
		Job: job.ToProto(),
	})
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err,
		}).Warning("grpc: Error calling SetJob")
		return err
	}
	return nil
}

// CallDeleteJob calls the leader passing the job name
func (grpcc *GRPCClient) CallDeleteJob(jobName string) (*Job, error) {
	var conn *grpc.ClientConn

	addr := grpcc.agent.raft.Leader()

	// Initiate a connection with the server
	conn, err := grpcc.Connect(string(addr))
	if err != nil {
		log.WithFields(logrus.Fields{
			"err":         err,
			"server_addr": addr,
		}).Error("grpc: error dialing.")
		return nil, err
	}
	defer conn.Close()

	// Synchronous call
	d := proto.NewDkronClient(conn)
	res, err := d.DeleteJob(context.Background(), &proto.DeleteJobRequest{
		JobName: jobName,
	})
	job := NewJobFromProto(res.Job)
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err,
		}).Warning("grpc: Error calling SetJob")
		return nil, err
	}
	return job, nil
}
