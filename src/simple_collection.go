package main

import (
	"errors"
	"sort"
)

var (
	KeyError   = errors.New("Key not found")
	ValueError = errors.New("Inappropriate value")
)

type SimpleDB struct {
	store map[string][]byte
}

func NewSimpleDB() *SimpleDB {
	return &SimpleDB{
		store: make(map[string][]byte),
	}
}

func (db SimpleDB) Get(key []byte) (value []byte, err error) {
	v, ok := db.store[string(key)]

	if !ok {
		return nil, KeyError
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
		return KeyError
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
		return nil, ValueError
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
	if s.index == len(s.keys)-1 {
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
