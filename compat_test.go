package backend

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func testBasic(t *testing.T, backend ...DB) {
	for _, db := range backend {
		// create key/value pairs
		txn, err := db.Txn()
		if err != nil {
			t.Fatalf("%s: begin writable transaction: %v", db.Name(), err)
		}
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key%.3d", i))
			val := []byte(fmt.Sprintf("val%.3d", i))
			if err = txn.Put(key, val); err != nil {
				t.Fatalf("%s: put key %q: %v", db.Name(), key, err)
			}
		}
		if err = txn.Commit(); err != nil {
			t.Fatalf("%s: commit writable transaction: %v", db.Name(), err)
		}

		// lookup key/value pair
		want := [][]byte{[]byte("key042"), []byte("key079")}
		got := make([][]byte, 0, 2)
		if err = db.BatchGet(want, func(key, val []byte) error {
			got = append(got, append([]byte(nil), key...))
			return nil
		}); err != nil {
			t.Fatalf("%s: batch get: %v", db.Name(), err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%s: batch get: expected keys %q, got %q", db.Name(), want, got)
		}

		// iterate key/value store
		iter, err := db.Iterator()
		if err != nil {
			t.Fatalf("%s: next iterator: %v", db.Name(), err)
		}
		i := 0
		for k, _ := iter.First(); k != nil; k, _ = iter.Next() {
			i++
		}
		if i != 100 {
			t.Fatalf("%s: iterator expected %d pairs, found %d", db.Name(), 100, i)
		}
	}
}

func testBasicTransaction(t *testing.T, backend ...DB) {
	for _, db := range backend {
		txn, err := db.Txn()
		if err != nil {
			t.Fatalf("%s: begin writable transaction: %v", db.Name(), err)
		}
		defer txn.Rollback()

		key := []byte("key042")
		val := []byte("val042")
		v, err := txn.Get(key)
		if err != nil {
			t.Fatalf("%s: transaction get key %q: %v", db.Name(), key, err)
		}
		if bytes.Compare(val, v) != 0 {
			t.Fatalf("%s: lookup execpted value %q, got %q", db.Name(), val, v)
		}

		val = []byte("abc")
		if err = txn.Put(key, val); err != nil {
			t.Fatalf("%s: put key %q: %v", db.Name(), key, err)
		}

		v, err = txn.Get(key)
		if err != nil {
			t.Fatalf("%s: transaction get key %q: %v", db.Name(), key, err)
		}
		if bytes.Compare(val, v) != 0 {
			t.Fatalf("%s: lookup execpted value %q, got %q", db.Name(), val, v)
		}

		v, err = txn.Get(val)
		if err != nil {
			t.Fatalf("%s: transaction get key %q: %v", db.Name(), val, err)
		}
		if v != nil {
			t.Fatalf("%s: transaction get: expected <nil> key", db.Name())
		}
	}
}

func TestCompatibility(t *testing.T) {
	boltDB := openBoltDB(t, "compatibility_boltdb.db")
	levelDB := openLevelDB(t, "compatibility_leveldb")
	defer func() {
		closeBoltDB(t, "compatibility_boltdb.db", boltDB)
		closeLevelDB(t, "compatibility_leveldb", levelDB)
	}()

	testBasic(t, boltDB, levelDB)
	testBasicTransaction(t, boltDB, levelDB)
}

func openBoltDB(t *testing.T, path string) *BoltDB {
	db, err := OpenBoltDB(path, 0)
	if err != nil {
		t.Errorf("opening BoltDB %q: %v", path, err)
	}
	return db
}

func closeBoltDB(t *testing.T, path string, db *BoltDB) {
	if err := db.Close(); err != nil {
		t.Errorf("closing BoltDB %q: %v", path, err)
	}
	os.RemoveAll(path)
}

func openLevelDB(t *testing.T, path string) *LevelDB {
	db, err := OpenLevelDB(path)
	if err != nil {
		t.Errorf("opening LevelDB %q: %v", path, err)
	}
	return db
}

func closeLevelDB(t *testing.T, path string, db *LevelDB) {
	if err := db.Close(); err != nil {
		t.Errorf("closing LevelDB %q: %v", path, err)
	}
	os.RemoveAll(path)
}
