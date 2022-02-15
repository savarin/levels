package main

import (
	"sort"
)

type KeyError struct{}

func (e *KeyError) Error() string {
	return "Key not found"
}

type ValueError struct{}

func (e *ValueError) Error() string {
	return "Inappropriate value"
}

type DB interface {
	// Get gets the value for the given key. It returns an error if the
	// DB does not contain the key.
	Get(key []byte) (value []byte, err error)

	// Has returns true if the DB contains the given key.
	Has(key []byte) (ret bool, err error)

	// Put sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	Put(key, value []byte) error

	// Delete deletes the value for the given key.
	Delete(key []byte) error

	// RangeScan returns an Iterator (see below) for scanning through all
	// key-value pairs in the given range, ordered by key ascending.
	RangeScan(start, limit []byte) (Iterator, error)
}

type Iterator interface {
	// Next moves the iterator to the next key/value pair.
	// It returns false if the iterator is exhausted.
	Next() bool

	// Error returns any accumulated error. Exhausting all the key/value pairs
	// is not considered to be an error.
	Error() error

	// Key returns the key of the current key/value pair, or nil if done.
	Key() []byte

	// Value returns the value of the current key/value pair, or nil if done.
	Value() []byte
}

type SimpleDB struct {
	store map[string][]byte
}

func (db SimpleDB) Get(key []byte) (value []byte, err error) {
	v, ok := db.store[string(key)]

	if !ok {
		return nil, &KeyError{}
	}

	return v, nil
}

func (db SimpleDB) Has(key []byte) (ret bool, err error) {
	_, ok := db.store[string(key)]
	return ok, nil
}

func (db SimpleDB) Put(key, value []byte) error {
	db.store[string(key)] = value
	return nil
}

func (db SimpleDB) Delete(key []byte) error {
	_, ok := db.store[string(key)]

	if !ok {
		return &KeyError{}
	}

	delete(db.store, string(key))
	return nil
}

func (db SimpleDB) RangeScan(start, limit []byte) (Iterator, error) {
	strings := make([]string, len(db.store))
	counter := 0

	for k, _ := range db.store {
		strings[counter] = k
		counter++
	}

	sort.Strings(strings)

	startString := string(start)
	limitString := string(limit)

	if startString > limitString {
		return nil, &ValueError{}
	}

	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	for _, key := range strings {
		if key >= startString && key < limitString {
			keys = append(keys, []byte(key))
			values = append(values, []byte(db.store[key]))
		}
	}

	return &SimpleIterator{
		keys:   keys,
		values: values,
		index:  0,
	}, nil
}

type SimpleIterator struct {
	keys   [][]byte
	values [][]byte
	index  int
}

func (s *SimpleIterator) Next() bool {
	if s.index == len(s.keys) - 1 {
		return false
	}

	s.index++
	return true
}

func (s *SimpleIterator) Error() error {
	return nil
}

func (s *SimpleIterator) Key() []byte {
	if len(s.keys) == 0 {
		return nil
	}

	return s.keys[s.index]
}

func (s *SimpleIterator) Value() []byte {
	if len(s.values) == 0 {
		return nil
	}

	return s.values[s.index]
}
