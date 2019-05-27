package dkron

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/abronan/valkeyrie/store"
	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/victorcoder/dkron/cron"
	dkronpb "github.com/victorcoder/dkron/proto"
)

const MaxExecutions = 100

type Store struct {
	agent *Agent
	db    *badger.DB
}

type JobOptions struct {
	ComputeStatus bool
	Tags          map[string]string `json:"tags"`
}

func NewStore(a *Agent, dir string) (*Store, error) {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:    db,
		agent: a,
	}, nil
}

// Store a job
func (s *Store) SetJob(job *Job, copyDependentJobs bool) error {
	//Existing job that has children, let's keep it's children

	// Sanitize the job name
	job.Name = generateSlug(job.Name)
	jobKey := fmt.Sprintf("jobs/%s", job.Name)

	// Init the job agent
	job.Agent = s.agent

	if err := s.validateJob(job); err != nil {
		return err
	}

	err := s.db.Update(func(txn *badger.Txn) error {
		// Get if the requested job already exist
		ej, err := s.GetJob(job.Name, nil)
		if err != nil && err != store.ErrKeyNotFound {
			return err
		}
		if ej != nil {
			// When the job runs, these status vars are updated
			// otherwise use the ones that are stored
			if ej.LastError.After(job.LastError) {
				job.LastError = ej.LastError
			}
			if ej.LastSuccess.After(job.LastSuccess) {
				job.LastSuccess = ej.LastSuccess
			}
			if ej.SuccessCount > job.SuccessCount {
				job.SuccessCount = ej.SuccessCount
			}
			if ej.ErrorCount > job.ErrorCount {
				job.ErrorCount = ej.ErrorCount
			}
			if len(ej.DependentJobs) != 0 && copyDependentJobs {
				job.DependentJobs = ej.DependentJobs
			}
		}

		pbj := job.ToProto()
		jb, err := proto.Marshal(pbj)
		if err != nil {
			return err
		}
		log.WithField("job", job.Name).Debug("store: Setting job")

		if err := txn.Set([]byte(jobKey), jb); err != nil {
			return err
		}

		if ej != nil {
			// Existing job that doesn't have parent job set and it's being set
			if ej.ParentJob == "" && job.ParentJob != "" {
				pj, err := job.GetParent()
				if err != nil {
					return err
				}

				pj.DependentJobs = append(pj.DependentJobs, job.Name)
				if err := s.SetJob(pj, false); err != nil {
					return err
				}
			}

			// Existing job that has parent job set and it's being removed
			if ej.ParentJob != "" && job.ParentJob == "" {
				pj, err := ej.GetParent()
				if err != nil {
					return err
				}

				ndx := 0
				for i, djn := range pj.DependentJobs {
					if djn == job.Name {
						ndx = i
						break
					}
				}
				pj.DependentJobs = append(pj.DependentJobs[:ndx], pj.DependentJobs[ndx+1:]...)
				if err := s.SetJob(pj, false); err != nil {
					return err
				}
			}
		}

		// New job that has parent job set
		if ej == nil && job.ParentJob != "" {
			pj, err := job.GetParent()
			if err != nil {
				return err
			}

			pj.DependentJobs = append(pj.DependentJobs, job.Name)
			if err := s.SetJob(pj, false); err != nil {
				return err
			}
		}

	})

	return nil
}

func (s *Store) validateTimeZone(timezone string) error {
	if timezone == "" {
		return nil
	}
	_, err := time.LoadLocation(timezone)
	return err
}

func (s *Store) AtomicJobPut(job *Job, prevJobKVPair *store.KVPair) (bool, error) {
	return true, nil
}

func (s *Store) validateJob(job *Job) error {
	if job.ParentJob == job.Name {
		return ErrSameParent
	}

	// Only validate the schedule if it doesn't have a parent
	if job.ParentJob == "" {
		if _, err := cron.Parse(job.Schedule); err != nil {
			return fmt.Errorf("%s: %s", ErrScheduleParse.Error(), err)
		}
	}

	if job.Concurrency != ConcurrencyAllow && job.Concurrency != ConcurrencyForbid && job.Concurrency != "" {
		return ErrWrongConcurrency
	}
	if err := s.validateTimeZone(job.Timezone); err != nil {
		return err
	}

	return nil
}

func (s *Store) jobHasTags(job *Job, tags map[string]string) bool {
	if job == nil || job.Tags == nil || len(job.Tags) == 0 {
		return false
	}

	res := true
	for k, v := range tags {
		var found bool

		if val, ok := job.Tags[k]; ok && v == val {
			found = true
		}

		res = res && found

		if !res {
			break
		}
	}

	return res
}

// GetJobs returns all jobs
func (s *Store) GetJobs(options *JobOptions) ([]*Job, error) {
	jobs := make([]*Job, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("jobs")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			v, err := item.Value()
			if err != nil {
				return err
			}

			var pbj dkronpb.Job
			if err := proto.Unmarshal(v, &pbj); err != nil {
				return err
			}
			job := NewJobFromProto(&pbj)

			job.Agent = s.agent
			if options != nil {
				if options.Tags != nil && len(options.Tags) > 0 && !s.jobHasTags(job, options.Tags) {
					continue
				}
				if options.ComputeStatus {
					job.Status = job.GetStatus()
				}
			}

			n, err := job.GetNext()
			if err != nil {
				return err
			}
			job.Next = n

			jobs = append(jobs, job)
		}
		return nil
	})

	return jobs, err
}

// Get a job
func (s *Store) GetJob(name string, options *JobOptions) (*Job, error) {
	job, _, err := s.GetJobWithKVPair(name, options)
	return job, err
}

func (s *Store) GetJobWithKVPair(name string, options *JobOptions) (*Job, *store.KVPair, error) {
	var job *Job

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("jobs/" + name))
		if err != nil {
			return err
		}

		res, err := item.Value()
		if err != nil {
			return err
		}
		var pbj dkronpb.Job
		if err := proto.Unmarshal(res, &pbj); err != nil {
			return err
		}
		job = NewJobFromProto(&pbj)

		log.WithFields(logrus.Fields{
			"job": job.Name,
		}).Debug("store: Retrieved job from datastore")

		job.Agent = s.agent
		if options != nil && options.ComputeStatus {
			job.Status = job.GetStatus()
		}

		n, err := job.GetNext()
		if err != nil {
			return err
		}
		job.Next = n
	})

	return job, nil, nil
}

func (s *Store) DeleteJob(name string) (*Job, error) {
	job, err := s.GetJob(name, nil)
	if err != nil {
		return nil, err
	}

	if err := s.DeleteExecutions(name); err != nil {
		if err != store.ErrKeyNotFound {
			return nil, err
		}
	}

	if err := s.client.Delete(s.keyspace + "/jobs/" + name); err != nil {
		return nil, err
	}

	return job, nil
}

func (s *Store) GetExecutions(jobName string) ([]*Execution, error) {
	prefix := fmt.Sprintf("%s/executions/%s", s.keyspace, jobName)
	res, err := s.client.List(prefix, nil)
	if err != nil {
		return nil, err
	}

	return s.unmarshalExecutions(res, jobName)
}

func (s *Store) GetLastExecutionGroup(jobName string) ([]*Execution, error) {
	executions, byGroup, err := s.GetGroupedExecutions(jobName)
	if err != nil {
		return nil, err
	}

	if len(executions) > 0 && len(byGroup) > 0 {
		return executions[byGroup[0]], nil
	}

	return nil, nil
}

// GetExecutionGroup returns all executions in the same group of a given execution
func (s *Store) GetExecutionGroup(execution *Execution) ([]*Execution, error) {
	res, err := s.GetExecutions(execution.JobName)
	if err != nil {
		return nil, err
	}

	var executions []*Execution
	for _, ex := range res {
		if ex.Group == execution.Group {
			executions = append(executions, ex)
		}
	}
	return executions, nil
}

// Returns executions for a job grouped and with an ordered index
// to facilitate access.
func (s *Store) GetGroupedExecutions(jobName string) (map[int64][]*Execution, []int64, error) {
	execs, err := s.GetExecutions(jobName)
	if err != nil {
		return nil, nil, err
	}
	groups := make(map[int64][]*Execution)
	for _, exec := range execs {
		groups[exec.Group] = append(groups[exec.Group], exec)
	}

	// Build a separate data structure to show in order
	var byGroup int64arr
	for key := range groups {
		byGroup = append(byGroup, key)
	}
	sort.Sort(sort.Reverse(byGroup))

	return groups, byGroup, nil
}

// SetExecution Save a new execution and returns the key of the new saved item or an error.
func (s *Store) SetExecution(execution *Execution) (string, error) {
	exJSON, _ := json.Marshal(execution)
	key := execution.Key()

	log.WithFields(logrus.Fields{
		"job":       execution.JobName,
		"execution": key,
	}).Debug("store: Setting key")

	err := s.client.Put(fmt.Sprintf("%s/executions/%s/%s", s.keyspace, execution.JobName, key), exJSON, nil)
	if err != nil {
		log.WithFields(logrus.Fields{
			"job":       execution.JobName,
			"execution": key,
			"error":     err,
		}).Debug("store: Failed to set key")
		return "", err
	}

	execs, err := s.GetExecutions(execution.JobName)
	if err != nil {
		log.WithError(err).
			WithField("job", execution.JobName).
			Error("store: Error no executions found for job")
	}

	// Delete all execution results over the limit, starting from olders
	if len(execs) > MaxExecutions {
		//sort the array of all execution groups by StartedAt time
		// TODO: Use sort.Slice
		sort.Sort(ExecList(execs))
		for i := 0; i < len(execs)-MaxExecutions; i++ {
			log.WithFields(logrus.Fields{
				"job":       execs[i].JobName,
				"execution": execs[i].Key(),
			}).Debug("store: to detele key")
			err := s.client.Delete(fmt.Sprintf("%s/executions/%s/%s", s.keyspace, execs[i].JobName, execs[i].Key()))
			if err != nil {
				log.WithError(err).
					WithField("execution", execs[i].Key()).
					Error("store: Error trying to delete overflowed execution")
			}
		}
	}

	return key, nil
}

func (s *Store) unmarshalExecutions(res []*store.KVPair, stopWord string) ([]*Execution, error) {
	var executions []*Execution
	for _, node := range res {
		if store.Backend(s.backend) != store.ZK {
			path := store.SplitKey(node.Key)
			dir := path[len(path)-2]
			if dir != stopWord {
				continue
			}
		}
		var execution Execution
		err := json.Unmarshal([]byte(node.Value), &execution)
		if err != nil {
			return nil, err
		}
		executions = append(executions, &execution)
	}
	return executions, nil
}

// Removes all executions of a job
func (s *Store) DeleteExecutions(jobName string) error {
	return s.client.DeleteTree(fmt.Sprintf("%s/executions/%s", s.keyspace, jobName))
}

// Retrieve the leader from the store
func (s *Store) GetLeader() []byte {
	res, err := s.client.Get(s.LeaderKey(), nil)
	if err != nil {
		if err == store.ErrNotReachable {
			log.Fatal("store: Store not reachable, be sure you have an existing key-value store running is running and is reachable.")
		} else if err != store.ErrKeyNotFound {
			log.Error(err)
		}
		return nil
	}

	log.WithField("node", string(res.Value)).Debug("store: Retrieved leader from datastore")

	return res.Value
}

// Retrieve the leader key used in the KV store to store the leader node
func (s *Store) LeaderKey() string {
	return s.keyspace + "/leader"
}
