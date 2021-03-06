package backend

/*
#cgo LDFLAGS:-lleveldb
#include <stdlib.h>
#include "leveldb/c.h"
*/
import "C"

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"unsafe"
)

type LevelOption func(*LevelDB) error

func WriteBufferSize(size int) LevelOption {
	return func(db *LevelDB) error {
		C.leveldb_options_set_write_buffer_size(db.opts, C.size_t(size))
		return nil
	}
}

func BlockSize(size int) LevelOption {
	return func(db *LevelDB) error {
		C.leveldb_options_set_block_size(db.opts, C.size_t(size))
		return nil
	}
}

func BlockRestartInterval(n int) LevelOption {
	return func(db *LevelDB) error {
		C.leveldb_options_set_block_restart_interval(db.opts, C.int(n))
		return nil
	}
}

var (
	cfalse = C.uchar(0)
	ctrue  = C.uchar(1)
)

const maxSlice = 0x7fffffff

func unsafeGoBytes(data *C.char, size C.size_t) []byte {
	dlen := C.int(size)
	if dlen > maxSlice {
		return C.GoBytes(unsafe.Pointer(data), dlen)
	}
	return (*[maxSlice]byte)(unsafe.Pointer(data))[:dlen:dlen]
}

func checkDatabaseError(errptr *C.char) error {
	if errptr == nil {
		return nil
	}
	err := Error(C.GoString(errptr))
	C.leveldb_free(unsafe.Pointer(errptr))
	return err
}

var _ DB = (*LevelDB)(nil)

type LevelDB struct {
	wopts  *C.leveldb_writeoptions_t // default txn write options
	opts   *C.leveldb_options_t      // default LevelDB options
	tree   *C.leveldb_t
	writer sync.Mutex // excluisve writer lock
}

func OpenLevelDB(root string, opts ...LevelOption) (*LevelDB, error) {
	db := &LevelDB{
		wopts: C.leveldb_writeoptions_create(),
		opts:  C.leveldb_options_create(),
	}
	C.leveldb_options_set_create_if_missing(db.opts, ctrue)

	for _, opt := range opts {
		if err := opt(db); err != nil {
			return nil, err
		}
	}

	path := C.CString(root)
	defer C.free(unsafe.Pointer(path))

	var errptr *C.char
	db.tree = C.leveldb_open(db.opts, path, &errptr)
	if err := checkDatabaseError(errptr); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *LevelDB) Close() error {
	if db == nil || db.tree == nil {
		return errors.New("closing unopened LevelDB instance")
	}
	C.leveldb_writeoptions_destroy(db.wopts)
	C.leveldb_options_destroy(db.opts)
	C.leveldb_close(db.tree)
	db.wopts = nil
	db.opts = nil
	db.tree = nil
	return nil
}

func (db *LevelDB) Name() string { return "LevelDB" }

func (db *LevelDB) WriteTo(w io.Writer) (int64, error) {
	panic("LevelDB: WriteTo not implemented")
}

func (db *LevelDB) Iterator() (Iterator, error) {
	return newLevelIterator(db, true), nil
}

func (db *LevelDB) Readonly() (Txn, error) {
	return newLevelTxn(db, false), nil
}

func (db *LevelDB) Writable() (RWTxn, error) {
	db.writer.Lock()
	return newLevelTxn(db, true), nil
}

type levelIterator struct {
	ropts *C.leveldb_readoptions_t
	snap  *C.leveldb_snapshot_t
	iter  *C.leveldb_iterator_t
	db    *LevelDB
}

func newLevelIterator(db *LevelDB, snapshot bool) *levelIterator {
	ropts := C.leveldb_readoptions_create()
	var snap *C.leveldb_snapshot_t
	if snapshot {
		snap := C.leveldb_create_snapshot(db.tree)
		C.leveldb_readoptions_set_snapshot(ropts, snap)
	}
	iter := C.leveldb_create_iterator(db.tree, ropts)
	return &levelIterator{
		ropts: ropts,
		snap:  snap,
		iter:  iter,
		db:    db,
	}
}

func (i *levelIterator) Close() error {
	if i == nil || i.db == nil {
		return errors.New("closing unopened iterator")
	}

	var errptr *C.char
	C.leveldb_iter_get_error(i.iter, &errptr)

	C.leveldb_iter_destroy(i.iter)
	C.leveldb_readoptions_destroy(i.ropts)
	if i.snap != nil {
		C.leveldb_release_snapshot(i.db.tree, i.snap)
		i.snap = nil
	}

	i.iter = nil
	i.ropts = nil
	i.db = nil
	return checkDatabaseError(errptr)
}

func (i levelIterator) isValid() bool {
	valid := C.leveldb_iter_valid(i.iter)
	if valid == cfalse {
		return false
	}
	return true
}

// get retrieves the key/value pair in the database. get simulates the
// leveldb Get method to avoid additional key/value copy.
func (i levelIterator) get(key []byte) ([]byte, error) {
	k := (*C.char)(unsafe.Pointer(&key[0]))
	klen := C.size_t(len(key))
	C.leveldb_iter_seek(i.iter, k, klen)
	if !i.isValid() {
		var errptr *C.char
		C.leveldb_iter_get_error(i.iter, &errptr)
		if errptr == nil {
			return nil, ErrNotFound
		}
		return nil, checkDatabaseError(errptr)
	}

	k = C.leveldb_iter_key(i.iter, &klen)
	if bytes.Compare(unsafeGoBytes(k, klen), key) != 0 {
		return nil, ErrNotFound
	}

	var vlen C.size_t
	v := C.leveldb_iter_value(i.iter, &vlen)
	return unsafeGoBytes(v, vlen), nil
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

type levelTxn struct {
	wopts    *C.leveldb_writeoptions_t
	batch    *C.leveldb_writebatch_t
	modified map[string][]byte
	iter     *levelIterator
	db       *LevelDB
	writable bool
}

func newLevelTxn(db *LevelDB, writable bool) *levelTxn {
	txn := &levelTxn{
		wopts:    C.leveldb_writeoptions_create(),
		batch:    C.leveldb_writebatch_create(),
		modified: make(map[string][]byte),
		db:       db,
	}
	if writable {
		txn.iter = newLevelIterator(db, false)
	} else {
		txn.iter = newLevelIterator(db, true)
	}
	return txn
}

// TODO: document internal iterator behaviour
func (t levelTxn) Get(key []byte) ([]byte, error) {
	v, found := t.modified[string(key)]
	if !found {
		return t.iter.get(key)
	}

	if v == nil { // deleted in transaction
		return nil, ErrNotFound
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
	k := (*C.char)(unsafe.Pointer(&key[0]))
	klen := C.size_t(len(key))

	C.leveldb_writebatch_delete(t.batch, k, klen)
	t.modified[string(key)] = nil
	return nil
}

func (t *levelTxn) close() error {
	C.leveldb_writebatch_destroy(t.batch)
	C.leveldb_writeoptions_destroy(t.wopts)
	err := t.iter.Close()
	t.wopts = nil
	t.batch = nil
	t.modified = nil
	if err != nil {
		return Error(err.Error())
	}
	return nil
}

func (t *levelTxn) Rollback() error {
	if t == nil || t.batch == nil {
		return errors.New("rollback unopened transaction")
	}

	if t.writable {
		t.db.writer.Unlock()
	}
	return t.close()
}

func (t *levelTxn) Commit() error {
	if t == nil || t.batch == nil {
		return errors.New("commit unopened transaction")
	}

	var errptr *C.char
	C.leveldb_write(t.db.tree, t.wopts, t.batch, &errptr)
	t.db.writer.Unlock()
	t.close() // TODO: error handling
	return checkDatabaseError(errptr)
}
