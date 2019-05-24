package dkron

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type dkronFSM struct {
	mu sync.Mutex
}

// Apply applies a Raft log entry to the key-value store.
func (f *dkronFSM) Apply(l *raft.Log) interface{} {
	return "noop"
}

func (f *dkronFSM) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	//f.kvs.Set([]byte(key), []byte(value))
	return nil
}

func (f *dkronFSM) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	//f.kvs.Delete([]byte(key))
	return nil
}

// Snapshot returns a snapshot of the key-value store. We wrap
// the things we need in fsmSnapshot and then send that over to Persist.
// Persist encodes the needed data from fsmsnapshot and transport it to
// Restore where the necessary data is replicated into the finite state machine.
// This allows the consensus algorithm to truncate the replicated log.
func (f *dkronFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the kvstore into a map for easy transport
	mapClone := make(map[string]string)
	// opt := badger.DefaultIteratorOptions
	// itr := f.kvs.kv.NewIterator(opt)
	// for itr.Rewind(); itr.Valid(); itr.Next() {
	// 	item := itr.Item()
	// 	mapClone[string(item.Key()[:])] = string(item.Value()[:])
	// }
	// itr.Close()

	return &dkronSnapshot{kvMap: mapClone}, nil
}

// Restore stores the key-value store to a previous state.
func (f *dkronFSM) Restore(kvMap io.ReadCloser) error {
	kvSnapshot := make(map[string]string)
	if err := json.NewDecoder(kvMap).Decode(&kvSnapshot); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	//for k, v := range kvSnapshot {
	//	f.kvs.Set([]byte(k), []byte(v))
	//}

	return nil
}

type dkronSnapshot struct {
	kvMap map[string]string
}

func (f *dkronSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.kvMap)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *dkronSnapshot) Release() {}
