package main

import (
	"log"
	"fmt"
	"github.com/dgraph-io/badger"
	"encoding/binary"
	"time"
)

func uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}
func bytesToUint64(b []byte) uint64 {
	// https://github.com/dgraph-io/badger/issues/449#issuecomment-378534988
	if len(b) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
// Merge function to add two uint64 numbers
func add(existing, new []byte) []byte {
	return uint64ToBytes(bytesToUint64(existing) + bytesToUint64(new))
}

func main() {

	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Merge Operations - https://github.com/dgraph-io/badger#merge-operations
	key := []byte("merge")

	fmt.Println("Before using merge... Check the current value, if exists...")
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		fmt.Printf("Value for the key %s: %s\n", key, val)
		return nil
	})

	fmt.Println("Try using merge... ")

	m := db.GetMergeOperator(key, add, 200*time.Millisecond)
	defer m.Stop()

	m.Add(uint64ToBytes(1))
	m.Add(uint64ToBytes(2))
	m.Add(uint64ToBytes(3))

	time.Sleep(1000 * time.Millisecond) // wait for merge to happen

	res, err := m.Get() // res should have value 6 encoded
	fmt.Println("Result value: ", bytesToUint64(res))
}