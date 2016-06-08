package backend

import "io"

type StorageError string

func (e StorageError) Error() string { return string(e) }

type DB interface {
	BatchGet(keys [][]byte, getter BatchGetter) error
	Get(key []byte, getter Getter) (bool, error)
	Iterator() (Iterator, error)

	Txn() (Txn, error)

	WriteTo(w io.Writer) (int64, error)
	Name() string
	Close() error
}

type BatchGetter func(key, value []byte) error

type Getter func(value []byte) error

type Txn interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error

	Rollback() error
	Commit() error
}

type Iterator interface {
	Seek(key []byte) ([]byte, []byte)
	First() ([]byte, []byte)
	Last() ([]byte, []byte)
	Next() ([]byte, []byte)
	Prev() ([]byte, []byte)
	Close() error
}
