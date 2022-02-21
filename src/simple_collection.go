package main

import (
	"io"
	"sort"
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
	startString := string(start)
	limitString := string(limit)

	if startString > limitString {
		return nil, ValueError
	}

	strings := make([]string, len(db.store))
	counter := 0

	for k, _ := range db.store {
		strings[counter] = k
		counter++
	}

	sort.Strings(strings)

	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	for _, key := range strings {
		if key >= startString && (len(limit) == 0 || key < limitString) {
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

func (db SimpleDB) Flush(w io.Writer) {
	Flush(db, w)
}

type SimpleIterator struct {
	keys   [][]byte
	values [][]byte
	index  int
}

func (iter *SimpleIterator) Next() bool {
	if len(iter.keys) == 0 || iter.index == len(iter.keys)-1 {
		return false
	}

	iter.index++
	return true
}

func (iter *SimpleIterator) Error() error {
	return nil
}

func (iter *SimpleIterator) Key() []byte {
	if len(iter.keys) == 0 {
		return nil
	}

	return iter.keys[iter.index]
}

func (iter *SimpleIterator) Value() []byte {
	if len(iter.values) == 0 {
		return nil
	}

	return iter.values[iter.index]
}
