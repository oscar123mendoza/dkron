package dkron

import (
	"time"

	"github.com/dgraph-io/badger"
)

type MessageType uint8

const (
	SetType MessageType = iota
	DeleteType

	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type BadgerKV struct {
	db *badger.DB
}

// NewBadgerKV returns a BadgerKV. If the directory
// in which the data will be stored does not exist,
// then that directory will be made for the user.
func NewBadgerKV(dir string) (*BadgerKV, error) {
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	bagderKV := &BadgerKV{
		db: db,
	}
	return bagderKV, nil
}

// Get a value using a key from a BadgerKV
func (b *BadgerKV) Get(key []byte) ([]byte, error) {
	var item *badger.Item
	err := b.db.View(func(txn *badger.Txn) error {
		i, err := txn.Get(key)
		item = i
		return err
	})
	if err != nil {
		return nil, err
	}

	return item.Value()
}

// Set a key-value pair in a BadgerKV
func (b *BadgerKV) Set(key, val []byte) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		txn.Set(key, val)
		return nil
	})
	return err
}

// Delete a key-value pair in a BadgerKV
func (b *BadgerKV) Delete(key []byte) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		txn.Delete(key)
		return nil
	})
	return err
}
