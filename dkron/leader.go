package dkron

import (
	"sync"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/serf/serf"
)

// monitorLeadership is used to monitor if we acquire or lose our role
// as the leader in the Raft cluster. There is some work the leader is
// expected to do, so we must react to changes
func (a *Agent) monitorLeadership() {
	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup
	for {
		select {
		case isLeader := <-a.leaderCh:
			switch {
			case isLeader:
				if weAreLeaderCh != nil {
					log.Error("agent: attempted to start the leader loop while running")
					continue
				}

				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					//a.leaderLoop(ch)
				}(weAreLeaderCh)
				log.Info("dkron: cluster leadership acquired")
				a.schedule()

			default:
				if weAreLeaderCh == nil {
					log.Error("dkron: attempted to stop the leader loop while not running")
					continue
				}

				log.Debug("dkron: shutting down leader loop")
				close(weAreLeaderCh)
				leaderLoop.Wait()
				weAreLeaderCh = nil
				log.Info("dkron: cluster leadership lost")
				a.sched.Stop()
			}

			//case <-a.shutdownCh:
			//	return
		}
	}
}

// reconcileMember is used to do an async reconcile of a single serf member
func (s *Server) reconcileMember(member serf.Member) error {
	// Check if this is a member we should handle
	valid, parts := isNomadServer(member)
	if !valid || parts.Region != s.config.Region {
		return nil
	}
	defer metrics.MeasureSince([]string{"dkron", "leader", "reconcileMember"}, time.Now())

	var err error
	switch member.Status {
	case serf.StatusAlive:
		err = s.addRaftPeer(member, parts)
	case serf.StatusLeft, StatusReap:
		err = s.removeRaftPeer(member, parts)
	}
	if err != nil {
		s.logger.Error("failed to reconcile member", "member", member, "error", err)
		return err
	}
	return nil
}