package dkron

import (
	"testing"
	"time"

	"github.com/hashicorp/serf/testutil"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGRPCExecutionDone(t *testing.T) {
	viper.Reset()

	aAddr := testutil.GetBindAddr().String()

	c := DefaultConfig()
	c.BindAddr = aAddr
	c.NodeName = "test1"
	c.Server = true
	c.LogLevel = logLevel

	a := NewAgent(c, nil)
	s, err := NewStore(a, "")
	if err != nil {
		t.Fatal(err)
	}
	a.Start()

	time.Sleep(2 * time.Second)

	testJob := &Job{
		Name:           "test",
		Schedule:       "@every 1m",
		Executor:       "shell",
		ExecutorConfig: map[string]string{"command": "/bin/false"},
		Disabled:       true,
	}

	if err := s.SetJob(testJob, true); err != nil {
		t.Fatalf("error creating job: %s", err)
	}

	testExecution := &Execution{
		JobName:    "test",
		Group:      time.Now().UnixNano(),
		StartedAt:  time.Now(),
		NodeName:   "testNode",
		FinishedAt: time.Now(),
		Success:    true,
		Output:     []byte("type"),
	}

	rc := NewGRPCClient(nil, a)
	rc.CallExecutionDone(a.getRPCAddr(), testExecution)
	execs, _ := s.GetExecutions("test")

	assert.Len(t, execs, 1)
	assert.Equal(t, string(testExecution.Output), string(execs[0].Output))

	// Test store execution on a deleted job
	s.DeleteJob(testJob.Name)

	testExecution.FinishedAt = time.Now()
	err = rc.CallExecutionDone(a.getRPCAddr(), testExecution)

	assert.Error(t, err, ErrExecutionDoneForDeletedJob)
}
