package backend

import (
	"errors"
	"io"
	"time"

	"github.com/boltdb/bolt"
)

var rootBucket = []byte("root")

var _ DB = (*BoltDB)(nil)

const defaultOpenMode = 0600

// BoltDB represents a key/value store.
type BoltDB struct {
	tree *bolt.DB
}

// OpenBoltDB creates and opens a database at the given path. If the file
// does not exist then it will be created automatically.
//
// Timeout is the amount of time to wait to obtain a file lock. When set
// to zero it will wait indefinitely. This option is only available on
// Darwin and Linux.
func OpenBoltDB(path string, timeout time.Duration) (*BoltDB, error) {
	tree, err := bolt.Open(path, defaultOpenMode, &bolt.Options{
		Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}

	if err = tree.Update(func(tx *bolt.Tx) (err error) {
		_, err = tx.CreateBucketIfNotExists(rootBucket)
		return err
	}); err != nil {
		return nil, errors.New("create root: " + err.Error())
	}

	return &BoltDB{tree: tree}, nil
}

func (db *BoltDB) Get(key []byte, value []byte) ([]byte, error) {
	err := db.tree.View(func(tx *bolt.Tx) error {
		val := tx.Bucket(rootBucket).Get(key)
		if val == nil {
			return ErrNotFound
		}
		value = clone(value, val)
		return nil
	})
	return value, err
}

func (db *BoltDB) Iterator() (Iterator, error) {
	tx, err := db.tree.Begin(false)
	if err != nil {
		return nil, err
	}
	return &boltIterator{c: tx.Bucket(rootBucket).Cursor(), tx: tx}, nil
}

func (db *BoltDB) Txn() (Txn, error) {
	tx, err := db.tree.Begin(true)
	if err != nil {
		return nil, err
	}
	return &boltTxn{b: tx.Bucket(rootBucket), tx: tx}, nil
}

func (db *BoltDB) WriteTo(w io.Writer) (n int64, err error) {
	err = db.tree.View(func(tx *bolt.Tx) (err error) {
		n, err = tx.WriteTo(w)
		return err
	})
	return n, err
}

func (db *BoltDB) Name() string { return "BoltDB" }

func (db *BoltDB) Close() error {
	if db == nil || db.tree == nil {
		return errors.New("closing unopened BoltDB instance")
	}
	err := db.tree.Close()
	db.tree = nil
	return err
}

type boltIterator struct {
	c  *bolt.Cursor
	tx *bolt.Tx
}

func (i *boltIterator) Seek(key []byte) ([]byte, []byte) {
	if i == nil || i.tx == nil {
		return nil, nil
	}
	return i.c.Seek(key)
}

func (i *boltIterator) First() ([]byte, []byte) {
	if i == nil || i.tx == nil {
		return nil, nil
	}
	return i.c.First()
}

func (i *boltIterator) Last() ([]byte, []byte) {
	if i == nil || i.tx == nil {
		return nil, nil
	}
	return i.c.Last()
}

func (i *boltIterator) Next() ([]byte, []byte) {
	if i == nil || i.tx == nil {
		return nil, nil
	}
	return i.c.Next()
}

func (i *boltIterator) Prev() ([]byte, []byte) {
	if i == nil || i.tx == nil {
		return nil, nil
	}
	return i.c.Next()
}

func (i *boltIterator) Close() error {
	if i == nil || i.tx == nil {
		return nil
	}
	err := i.tx.Rollback()
	i.tx = nil
	return err
}

type boltTxn struct {
	b  *bolt.Bucket
	tx *bolt.Tx
}

func (t *boltTxn) Put(key, value []byte) error {
	if t == nil || t.tx == nil {
		return nil
	}
	return t.b.Put(key, value)
}

func (t *boltTxn) Delete(key []byte) error {
	if t == nil || t.tx == nil {
		return nil
	}
	return t.b.Delete(key)
}

func (t *boltTxn) Get(key []byte) ([]byte, error) {
	if t == nil || t.tx == nil {
		return nil, nil
	}
	value := t.b.Get(key)
	if value == nil {
		return nil, ErrNotFound
	}
	return value, nil
}

func (t *boltTxn) Rollback() error {
	if t == nil || t.tx == nil {
		return nil
	}
	err := t.tx.Rollback()
	t.tx = nil
	return err
}

func (t *boltTxn) Commit() error {
	if t == nil || t.tx == nil {
		return nil
	}
	err := t.tx.Commit()
	t.tx = nil
	return err
}
