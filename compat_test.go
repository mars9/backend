package backend

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"
)

var (
	compatValues     = [][]byte{}
	compatKeys       = [][]byte{}
	compatPairLength = 100
)

func init() {
	for i := 0; i < compatPairLength; i++ {
		key := []byte(fmt.Sprintf("key%.3d", i))
		val := []byte(fmt.Sprintf("val%.3d", i))
		compatValues = append(compatValues, val)
		compatKeys = append(compatKeys, key)
	}
}

func testBasic(t *testing.T, backend ...DB) {
	for _, db := range backend {
		// create key/value pairs
		txn, err := db.Writable()
		if err != nil {
			t.Fatalf("%s: begin writable transaction: %v", db.Name(), err)
		}

		// insert key/value pairs
		for i, key := range compatKeys {
			if err = txn.Put(key, compatValues[i]); err != nil {
				t.Fatalf("%s: put key %q: %v", db.Name(), key, err)
			}
		}
		if err = txn.Commit(); err != nil {
			t.Fatalf("%s: commit writable transaction: %v", db.Name(), err)
		}

		rtxn, err := db.Readonly()
		if err != nil {
			t.Fatalf("%s: begin readonly transaction: %v", db.Name(), err)
		}
		// lookup key/value pairs
		want := [][]byte{[]byte("val042"), []byte("val079")}
		got := make([][]byte, 0, 2)
		for _, key := range [][]byte{[]byte("key042"), []byte("key079")} {
			val, err := rtxn.Get(key)
			if err != nil {
				t.Fatalf("%s: get: %v", db.Name(), err)
			}
			got = append(got, val)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("%s: get: expected keys %q, got %q", db.Name(), want, got)
		}

		// returns ErrNotFound
		val, err := rtxn.Get([]byte("xxx"))
		if err != ErrNotFound {
			t.Fatalf("%s: get: expected ErrNotFound, got %v", db.Name(), err)
		}
		if val != nil {
			t.Fatalf("%s: get: expected <nil> value, got %q", db.Name, val)
		}
		if err = rtxn.Rollback(); err != nil {
			t.Fatalf("%s: rollback readonly transaction: %v", db.Name(), err)
		}

		// iterate key/value store
		iter, err := db.Iterator()
		if err != nil {
			t.Fatalf("%s: next iterator: %v", db.Name(), err)
		}
		defer iter.Close()

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
		txn, err := db.Writable()
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
		if err != ErrNotFound {
			t.Fatalf("%s: transaction get: expected ErrNotFound, got %v", db.Name(), err)
		}
	}
}

func testBasicIterator(t *testing.T, backend ...DB) {
	for _, db := range backend {
		iter, err := db.Iterator()
		if err != nil {
			t.Fatalf("%s: next iterator: %v", db.Name(), err)
		}

		i := 0
		for k, v := iter.First(); k != nil; k, v = iter.Next() {
			val := compatValues[i]
			key := compatKeys[i]
			if bytes.Compare(v, val) != 0 {
				t.Fatalf("%s: ascending iterator: expected value %q, got %q", db.Name(), val, v)
			}
			if bytes.Compare(k, key) != 0 {
				t.Fatalf("%s: ascending iterator: expected key %q, got %q", db.Name(), key, k)
			}
			i++
		}

		i = compatPairLength - 1
		for k, v := iter.Last(); k != nil; k, v = iter.Prev() {
			val := compatValues[i]
			key := compatKeys[i]
			if bytes.Compare(v, val) != 0 {
				t.Fatalf("%s: descending iterator: expected value %q, got %q", db.Name(), val, v)
			}
			if bytes.Compare(k, key) != 0 {
				t.Fatalf("%s: descending iterator: expected key %q, got %q", db.Name(), key, k)
			}
			i--
		}

		i = compatPairLength / 2
		k, v := iter.Seek(compatKeys[i])
		val := compatValues[i]
		key := compatKeys[i]
		if bytes.Compare(v, val) != 0 {
			t.Fatalf("%s: seek: expected value %q, got %q", db.Name(), val, v)
		}
		if bytes.Compare(k, key) != 0 {
			t.Fatalf("%s: seek: expected key %q, got %q", db.Name(), key, k)
		}

		k, v = iter.Seek([]byte("xxx"))
		if v != nil {
			t.Fatalf("%s: seek: expected <nil> value, got %q", db.Name(), v)
		}
		if v != nil {
			t.Fatalf("%s: seek: expected <nil> key, got %q", db.Name(), k)
		}

		if err = iter.Close(); err != nil {
			t.Fatalf("%s: closing iterator: %v", db.Name(), err)
		}
	}
}

func TestCompatibility(t *testing.T) {
	boltDB := openBoltDB(t, "compatibility_boltdb.db")
	//levelDB := openLevelDB(t, "compatibility_leveldb")
	defer func() {
		closeBoltDB(t, "compatibility_boltdb.db", boltDB)
		//	closeLevelDB(t, "compatibility_leveldb", levelDB)
	}()

	testBasic(t, boltDB)
	testBasicTransaction(t, boltDB)
	testBasicIterator(t, boltDB)
	//	testBasic(t, boltDB, levelDB)
	//	testBasicTransaction(t, boltDB, levelDB)
	//	testBasicIterator(t, boltDB, levelDB)
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

/*
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
*/
