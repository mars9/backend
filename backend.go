package backend

import "io"

// Iterator represents an iterator that can traverse over all key/value
// pairs in a database. Keys and values returned from the iterator are
// only valid for the life of the transaction. An iterator must be closed
// after use, but it is not necessary to read an iterator until
// exhaustion.
//
// An iterator is not necessarily goroutine-safe, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
type Iterator interface {
	// Seek moves the iterator to a given key and returns it. If the key
	// does not exist then the next key is used. If no keys follow, a nil
	// key is returned. The returned key and value are only valid for the
	// life of the transaction.
	Seek(key []byte) ([]byte, []byte)

	// First moves the iterator to the first item in the database and
	// returns its key and value. The returned key and value are only valid
	// for the life of the transaction.
	First() ([]byte, []byte)

	// Last moves the iterator to the last item in the database and
	// returns its key and value. The returned key and value are only valid
	// for the life of the transaction.
	Last() ([]byte, []byte)

	// Next moves the iterator to the next item in the database and returns
	// its key and value. If the iterator is at the end of the database
	// then a nil key and value are returned. The returned key and value
	// are only valid for the life of the transaction.
	Next() ([]byte, []byte)

	// Prev moves the iterator to the previous item in the database and
	// returns its key and value. If the iterator is at the beginning of
	// the database then a nil key and value are returned. The returned key
	// and value are only valid for the life of the transaction.
	Prev() ([]byte, []byte)

	// Close closes the iterator and returns any accumulated error.
	// Exhausting all the key/value pairs in a table is not considered to
	// be an error. It is valid to call Close multiple times. Other methods
	// should not be called after the iterator has been closed.
	Close() error
}

// Txn represents a read-only transaction on the database.
type Txn interface {
	// Get gets the value for the given key. It returns ErrNotFound if the
	// database does not contain the key.
	//
	// The caller should not modify the contents of the returned slice, but
	// it is safe to modify the contents of the argument after Get returns.
	Get(key []byte) ([]byte, error)

	// Rollback closes the transaction and ignores all previous updates.
	Rollback() error
}

// RWTxn represents a read/write transaction on the database.
type RWTxn interface {
	Txn

	// Put sets the value for the given key. If the key exist then its
	// previous value will be overwritten. Supplied value must remain valid
	// for the life of the transaction.
	Put(key, value []byte) error

	// Delete deletes the value for the given key. If the key does not
	// exist then nothing is done and a nil error is returned.
	//
	// It is safe to modify the contents of the arguments after Delete
	// returns.
	Delete(key []byte) error

	// Commit write all changes.
	Commit() error
}

// DB represents a key/value store.
type DB interface {
	// Iterator creates a iterator associated with the database.
	Iterator() (Iterator, error)

	// Readonly starts a new read-only transaction.  Starting multiple
	// read-only transaction will not block.
	Readonly() (Txn, error)

	// Writable starts a new transaction. Only one write transaction can be
	// used at a time. Starting multiple write transactions will cause the
	// calls to block and be serialized until the current write transaction
	// finishes.
	//
	// Transactions should not be dependent on one another.
	Writable() (RWTxn, error)

	// WriteTo writes the entire database to a writer.
	WriteTo(w io.Writer) (int64, error)

	// Name returns the unique database name.
	Name() string

	// Close closes the DB. It may or may not close any underlying io.Reader
	// or io.Writer, depending on how the DB was created.
	//
	// It is not safe to close a DB until all outstanding iterators and
	// transactions are closed. It is valid to call Close multiple times.
	// Other methods should not be called after the DB has been closed.
	Close() error
}

// Error represents a database error.
type Error string

func (e Error) Error() string { return string(e) }

// ErrNotFound means that a get or delete call did not find the requested
// key.
const ErrNotFound Error = Error("key not found")
