package main

import (
	"errors"
	"testing"
)

type entry struct {
	Key   []byte
	Value []byte
}

var (
	A = entry{Key: []byte("a"), Value: []byte("alpha")}
	B = entry{Key: []byte("b"), Value: []byte("bravo")}
	C = entry{Key: []byte("c"), Value: []byte("charlie")}
)

func TestRun(t *testing.T) {
	testRun(t, func() DB { return NewSimpleDB() })
	testRun(t, func() DB { return NewLinkedListDB() })
}

func testRun(t *testing.T, factory func() DB) {
	db := factory()

	for _, e := range []entry{A, B, C} {
		err := db.Put(e.Key, e.Value)
		if err != nil {
			t.Fatalf("unexpected error when putting key %q with value %q: %s", e.Key, e.Value, err)
		}
	}

	for _, e := range []entry{A, B, C} {
		v, err := db.Get(e.Key)
		if err != nil {
			t.Fatalf("unexpected error when getting key %q: %s", e.Key, err)
		}

		if string(v) != string(e.Value) {
			t.Fatalf("expected %q got %q: %s", v, e.Value, err)
		}
	}

	err := db.Delete(B.Key)
	if err != nil {
		t.Fatalf("unexpected error when deleting key %q: %s", B.Key, err)
	}

	for _, e := range []entry{A, C} {
		v, err := db.Get(e.Key)
		if err != nil {
			t.Fatalf("unexpected error when getting key %q: %s", e.Key, err)
		}

		if string(v) != string(e.Value) {
			t.Fatalf("expected %q got %q: %s", v, e.Value, err)
		}
	}

	_, err = db.Get(B.Key)
	if !errors.Is(err, KeyError) {
		t.Errorf("e")
	}
}
