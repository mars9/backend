package backend

/*
#cgo LDFLAGS: -lleveldb
#include <stdlib.h>
#include "leveldb/c.h"
*/
import "C"

import (
	"bytes"
	"io"
	"sync"
	"unsafe"
)

type StorageError string

func (e StorageError) Error() string { return string(e) }

/*
type Option func(*LevelDB) error

func CreateIfMissing() Option {
	return func(db *LevelDB) error {
		C.leveldb_options_set_create_if_missing(db.opts, C.uchar(1))
		return nil
	}
}

func ErrorIfExists() Option {
	return func(db *LevelDB) error {
		C.leveldb_options_set_error_if_exists(db.opts, C.uchar(1))
		return nil
	}
}

func WriteBufferSize(size int) Option {
	return func(db *LevelDB) error {
		C.leveldb_options_set_write_buffer_size(db.opts, C.size_t(size))
		return nil
	}
}

func MaxOpenFiles(max int) Option {
	return func(db *LevelDB) error {
		C.leveldb_options_set_max_open_files(db.opts, C.int(max))
		return nil
	}
}

func BlockSize(size int) Option {
	return func(db *LevelDB) error {
		C.leveldb_options_set_block_size(db.opts, C.size_t(size))
		return nil
	}
}

func BlockRestartInterval(n int) Option {
	return func(db *LevelDB) error {
		C.leveldb_options_set_block_restart_interval(db.opts, C.int(n))
		return nil
	}
}
*/

func unsafeGoBytes(data *C.char, size C.size_t) []byte {
	const maxLen = 0x7fffffff

	dlen := C.int(size)
	if dlen > maxLen {
		return C.GoBytes(unsafe.Pointer(data), dlen)
	}

	return (*[maxLen]byte)(unsafe.Pointer(data))[:dlen:dlen]
}

func errptrToGoError(errptr *C.char) error {
	if errptr == nil {
		return nil
	}

	err := StorageError(C.GoString(errptr))
	C.leveldb_free(unsafe.Pointer(errptr))
	return err
}

var _ DB = (*LevelDB)(nil)

type LevelDB struct {
	wopts  *C.leveldb_writeoptions_t // default write options, use by levelTxn
	opts   *C.leveldb_options_t      // default LevelDB options
	tree   *C.leveldb_t
	writer sync.Mutex // excluisve writer mutex
}

func OpenLevelDB(directory string) (*LevelDB, error) {
	db := &LevelDB{
		wopts: C.leveldb_writeoptions_create(),
		opts:  C.leveldb_options_create(),
	}

	//for _, opt := range options {
	//	if err := opt(db); err != nil {
	//		return nil, err
	//	}
	//}

	// TODO: should be optional
	C.leveldb_options_set_create_if_missing(db.opts, C.uchar(1))

	path := C.CString(directory)
	defer C.free(unsafe.Pointer(path))

	var errptr *C.char
	db.tree = C.leveldb_open(db.opts, path, &errptr)
	if err := errptrToGoError(errptr); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *LevelDB) Close() error {
	C.leveldb_writeoptions_destroy(db.wopts)
	C.leveldb_options_destroy(db.opts)
	C.leveldb_close(db.tree)
	return nil
}

func (db *LevelDB) Name() string { return "LevelDB" }

func (db *LevelDB) WriteTo(w io.Writer) (int64, error) {
	panic("LevelDB: WriteTo not implemented")
}

// TODO: remove duplicate code!

func (db *LevelDB) BatchGet(keys [][]byte, getter BatchGetter) error {
	snap := C.leveldb_create_snapshot(db.tree) // create reader snapshot
	ropts := C.leveldb_readoptions_create()
	C.leveldb_readoptions_set_snapshot(ropts, snap)
	defer func() {
		C.leveldb_readoptions_destroy(ropts)
		C.leveldb_release_snapshot(db.tree, snap)
	}()

	for _, key := range keys {
		k := (*C.char)(unsafe.Pointer(&key[0]))
		klen := C.size_t(len(key))
		var errptr *C.char
		var vlen C.size_t

		v := C.leveldb_get(db.tree, ropts, k, klen, &vlen, &errptr)
		if err := errptrToGoError(errptr); err != nil {
			return err
		}
		if v == nil {
			return getter(key, nil)
		}

		err := getter(key, unsafeGoBytes(v, vlen))
		C.leveldb_free(unsafe.Pointer(v))
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *LevelDB) Get(key []byte, getter Getter) (bool, error) {
	snap := C.leveldb_create_snapshot(db.tree) // create reader snapshot
	ropts := C.leveldb_readoptions_create()
	C.leveldb_readoptions_set_snapshot(ropts, snap)
	defer func() {
		C.leveldb_readoptions_destroy(ropts)
		C.leveldb_release_snapshot(db.tree, snap)
	}()

	k := (*C.char)(unsafe.Pointer(&key[0]))
	klen := C.size_t(len(key))
	var errptr *C.char
	var vlen C.size_t

	v := C.leveldb_get(db.tree, ropts, k, klen, &vlen, &errptr)
	if err := errptrToGoError(errptr); err != nil {
		return v != nil, err
	}
	if v == nil {
		return false, getter(nil)
	}

	err := getter(unsafeGoBytes(v, vlen))
	C.leveldb_free(unsafe.Pointer(v))
	return true, err
}

func (db *LevelDB) Iterator() (Iterator, error) {
	snap := C.leveldb_create_snapshot(db.tree) // create reader snapshot
	ropts := C.leveldb_readoptions_create()
	C.leveldb_readoptions_set_snapshot(ropts, snap)

	return &levelIterator{
		iter:  C.leveldb_create_iterator(db.tree, ropts),
		ropts: ropts,
		snap:  snap,
		tree:  db.tree,
	}, nil
}

func (db *LevelDB) Txn() (Txn, error) {
	db.writer.Lock()
	return &levelTxn{
		wopts:    C.leveldb_writeoptions_create(),
		ropts:    C.leveldb_readoptions_create(),
		batch:    C.leveldb_writebatch_create(),
		tree:     db.tree,
		modified: make(map[string][]byte),
		writer:   &db.writer,
	}, nil
}

type levelIterator struct {
	ropts *C.leveldb_readoptions_t
	snap  *C.leveldb_snapshot_t
	iter  *C.leveldb_iterator_t
	tree  *C.leveldb_t
}

var cfalse = C.uchar(0)

func (i levelIterator) isValid() bool {
	valid := C.leveldb_iter_valid(i.iter)
	if valid == cfalse {
		return false
	}
	return true
}

// current returns the key/value pair in the database the levelIterator
// currently holds. If the leveldb iterator is not valid current panics.
func (i levelIterator) current() ([]byte, []byte) {
	var klen, vlen C.size_t
	k := C.leveldb_iter_key(i.iter, &klen)
	v := C.leveldb_iter_value(i.iter, &vlen)
	return unsafeGoBytes(k, klen), unsafeGoBytes(v, vlen)
}

func (i levelIterator) Seek(key []byte) ([]byte, []byte) {
	k := (*C.char)(unsafe.Pointer(&key[0]))
	klen := C.size_t(len(key))
	C.leveldb_iter_seek(i.iter, k, klen)
	if !i.isValid() {
		return nil, nil
	}
	return i.current()
}

func (i levelIterator) First() ([]byte, []byte) {
	C.leveldb_iter_seek_to_first(i.iter)
	if !i.isValid() {
		return nil, nil
	}
	return i.current()
}

func (i levelIterator) Last() ([]byte, []byte) {
	C.leveldb_iter_seek_to_last(i.iter)
	if !i.isValid() {
		return nil, nil
	}
	return i.current()
}

func (i levelIterator) Next() ([]byte, []byte) {
	if !i.isValid() {
		return nil, nil
	}
	C.leveldb_iter_next(i.iter)
	if !i.isValid() {
		return nil, nil
	}
	return i.current()
}

func (i levelIterator) Prev() ([]byte, []byte) {
	if !i.isValid() {
		return nil, nil
	}
	C.leveldb_iter_prev(i.iter)
	if !i.isValid() {
		return nil, nil
	}
	return i.current()
}

func (i *levelIterator) Close() error {
	C.leveldb_iter_destroy(i.iter)
	C.leveldb_readoptions_destroy(i.ropts)
	C.leveldb_release_snapshot(i.tree, i.snap)
	i.iter = nil
	i.ropts = nil
	i.snap = nil
	return nil
}

type levelTxn struct {
	wopts    *C.leveldb_writeoptions_t
	ropts    *C.leveldb_readoptions_t
	batch    *C.leveldb_writebatch_t
	tree     *C.leveldb_t
	modified map[string][]byte
	writer   *sync.Mutex
}

// TODO: document internal iterator behaviour
func (t levelTxn) Get(key []byte) ([]byte, error) {
	v, found := t.modified[string(key)]
	if !found {
		iter := C.leveldb_create_iterator(t.tree, t.ropts)
		defer C.leveldb_iter_destroy(iter)

		k := (*C.char)(unsafe.Pointer(&key[0]))
		klen := C.size_t(len(key))
		C.leveldb_iter_seek(iter, k, klen)

		valid := C.leveldb_iter_valid(iter)
		if valid == cfalse {
			return nil, nil
		}

		k = C.leveldb_iter_key(iter, &klen)
		if bytes.Compare(key, unsafeGoBytes(k, klen)) != 0 {
			return nil, nil
		}

		var vlen C.size_t
		v := C.leveldb_iter_value(iter, &vlen)

		return unsafeGoBytes(v, vlen), nil
	}
	return v, nil
}

func (t *levelTxn) Put(key, value []byte) error {
	k := (*C.char)(unsafe.Pointer(&key[0]))
	v := (*C.char)(unsafe.Pointer(&value[0]))
	klen := C.size_t(len(key))
	vlen := C.size_t(len(value))

	C.leveldb_writebatch_put(t.batch, k, klen, v, vlen)
	t.modified[string(key)] = value
	return nil
}

func (t levelTxn) Delete(key []byte) error {
	return nil
}

func (t *levelTxn) close() {
	C.leveldb_writebatch_destroy(t.batch)
	C.leveldb_writeoptions_destroy(t.wopts)
	C.leveldb_readoptions_destroy(t.ropts)
	t.wopts = nil
	t.ropts = nil
	t.batch = nil
	t.modified = nil
}

func (t *levelTxn) Rollback() error {
	t.writer.Unlock()
	t.close()
	return nil
}

func (t *levelTxn) Commit() error {
	var errptr *C.char
	C.leveldb_write(t.tree, t.wopts, t.batch, &errptr)
	t.writer.Unlock()
	t.close()
	return errptrToGoError(errptr)
}
