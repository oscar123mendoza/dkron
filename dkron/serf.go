package dkron

import (
	"github.com/hashicorp/serf/serf"
)

const (
	// StatusReap is used to update the status of a node if we
	// are handling a EventMemberReap
	StatusReap = serf.MemberStatus(-1)
)

// localMemberEvent is used to reconcile Serf events with the
// consistent store if we are the current leader.
func (a *Agent) localMemberEvent(me serf.MemberEvent) {
	// Do nothing if we are not the leader
	if !a.IsLeader() {
		return
	}

	// Check if this is a reap event
	isReap := me.EventType() == serf.EventMemberReap

	// Queue the members for reconciliation
	for _, m := range me.Members {
		// Change the status if this is a reap event
		if isReap {
			m.Status = StatusReap
		}
		select {
		case a.reconcileCh <- m:
		default:
		}
	}
}
