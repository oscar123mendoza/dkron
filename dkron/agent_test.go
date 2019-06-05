package dkron

import (
	"testing"
	"time"

	"github.com/hashicorp/serf/testutil"
	"github.com/stretchr/testify/assert"
)

var (
	logLevel = "error"
)

func TestAgentCommand_runForElection(t *testing.T) {
	a1Name := "test1"
	a2Name := "test2"
	a1Addr := testutil.GetBindAddr().String()
	a2Addr := testutil.GetBindAddr().String()

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)

	// Override leader TTL
	defaultLeaderTTL = 2 * time.Second

	c := DefaultConfig()
	c.BindAddr = a1Addr
	c.StartJoin = []string{a2Addr}
	c.NodeName = a1Name
	c.Server = true
	c.LogLevel = logLevel
	c.BootstrapExpect = 3
	c.DevMode = true

	a1 := NewAgent(c, nil)
	if err := a1.Start(); err != nil {
		t.Fatal(err)
	}

	// Wait for the first agent to start and elect itself as leader
	if a1.IsLeader() {
		m, err := a1.leaderMember()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s is the current leader", m.Name)
		assert.Equal(t, a1Name, m.Name)
	}

	// Start another agent
	c = DefaultConfig()
	c.BindAddr = a2Addr
	c.StartJoin = []string{a1Addr + ":8946"}
	c.NodeName = a2Name
	c.Server = true
	c.LogLevel = logLevel
	c.BootstrapExpect = 3
	c.DevMode = true

	a2 := NewAgent(c, nil)
	a2.Start()

	// Start another agent
	c = DefaultConfig()
	c.BindAddr = testutil.GetBindAddr().String()
	c.StartJoin = []string{a1Addr + ":8946"}
	c.NodeName = "test3"
	c.Server = true
	c.LogLevel = logLevel
	c.BootstrapExpect = 3
	c.DevMode = true

	a3 := NewAgent(c, nil)
	a3.Start()

	time.Sleep(2 * time.Second)

	// Send a shutdown request
	a1.Stop()

	// Wait until a follower steps as leader
	time.Sleep(2 * time.Second)
	assert.True(t, (a2.IsLeader() || a3.IsLeader()))
	log.Info(a3.IsLeader())

	a2.Stop()
	a3.Stop()
}

func Test_processFilteredNodes(t *testing.T) {
	a1Addr := testutil.GetBindAddr().String()
	a2Addr := testutil.GetBindAddr().String()

	c := DefaultConfig()
	c.BindAddr = a1Addr
	c.StartJoin = []string{a2Addr}
	c.NodeName = "test1"
	c.Server = true
	c.LogLevel = logLevel
	c.Tags = map[string]string{"tag": "test"}
	c.DevMode = true

	a1 := NewAgent(c, nil)
	a1.Start()

	time.Sleep(2 * time.Second)

	// Start another agent
	c = DefaultConfig()
	c.BindAddr = a2Addr
	c.StartJoin = []string{a1Addr + ":8946"}
	c.NodeName = "test2"
	c.Server = true
	c.LogLevel = logLevel
	c.Tags = map[string]string{"tag": "test"}
	c.DevMode = true

	a2 := NewAgent(c, nil)
	a2.Start()

	time.Sleep(2 * time.Second)

	job := &Job{
		Name: "test_job_1",
		Tags: map[string]string{
			"foo": "bar:1",
			"tag": "test:2",
		},
	}

	nodes, tags, err := a1.processFilteredNodes(job)
	if err != nil {
		t.Fatal(err)
	}

	assert.Contains(t, nodes, "test1")
	assert.Contains(t, nodes, "test2")
	assert.Equal(t, tags["tag"], "test")
	assert.Equal(t, job.Tags["tag"], "test:2")
	assert.Equal(t, job.Tags["foo"], "bar:1")

	a1.Stop()
	a2.Stop()
}

func TestEncrypt(t *testing.T) {
	c := DefaultConfig()
	c.BindAddr = testutil.GetBindAddr().String()
	c.NodeName = "test1"
	c.Server = true
	c.Tags = map[string]string{"role": "test"}
	c.EncryptKey = "kPpdjphiipNSsjd4QHWbkA=="
	c.LogLevel = logLevel
	c.DevMode = true

	a := NewAgent(c, nil)
	a.Start()

	time.Sleep(2 * time.Second)

	assert.True(t, a.serf.EncryptionEnabled())
	a.Stop()
}

func Test_getRPCAddr(t *testing.T) {
	a1Addr := testutil.GetBindAddr()

	c := DefaultConfig()
	c.BindAddr = a1Addr.String() + ":5000"
	c.NodeName = "test1"
	c.Server = true
	c.Tags = map[string]string{"role": "test"}
	c.LogLevel = logLevel
	c.DevMode = true

	a := NewAgent(c, nil)
	a.Start()

	time.Sleep(2 * time.Second)

	getRPCAddr := a.getRPCAddr()
	exRPCAddr := a1Addr.String() + ":6868"

	assert.Equal(t, exRPCAddr, getRPCAddr)
	a.Stop()
}

func TestAgentConfig(t *testing.T) {
	advAddr := testutil.GetBindAddr().String()

	c := DefaultConfig()
	c.BindAddr = testutil.GetBindAddr().String()
	c.AdvertiseAddr = advAddr
	c.LogLevel = logLevel

	a := NewAgent(c, nil)
	a.Start()

	time.Sleep(2 * time.Second)

	assert.NotEqual(t, a.config.AdvertiseAddr, a.config.BindAddr)
	assert.NotEmpty(t, a.config.AdvertiseAddr)
	assert.Equal(t, advAddr, a.config.AdvertiseAddr)

	a.Stop()
}
