package dkron

import (
	"github.com/abronan/valkeyrie/store"
)

type Storage interface {
	SetJob(job *Job, copyDependentJobs bool) error
	AtomicJobPut(job *Job, prevJobKVPair *store.KVPair) (bool, error)
	DeleteJob(name string) (*Job, error)
	SetExecution(execution *Execution) (string, error)
	DeleteExecutions(jobName string) error

	GetJobs(options *JobOptions) ([]*Job, error)
	GetJob(name string, options *JobOptions) (*Job, error)
	GetJobWithKVPair(name string, options *JobOptions) (*Job, *store.KVPair, error)
	GetExecutions(jobName string) ([]*Execution, error)
	GetLastExecutionGroup(jobName string) ([]*Execution, error)
	GetExecutionGroup(execution *Execution) ([]*Execution, error)
	GetGroupedExecutions(jobName string) (map[int64][]*Execution, []int64, error)
}
