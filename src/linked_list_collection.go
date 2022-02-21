package main

import (
	"io"
)

type linkedListNode struct {
	item Item
	next *linkedListNode
	prev *linkedListNode
}

type LinkedListDB struct {
	head *linkedListNode
	tail *linkedListNode
}

func NewLinkedListDB() *LinkedListDB {
	head := &linkedListNode{}
	tail := &linkedListNode{}
	head.next = tail
	tail.prev = head
	return &LinkedListDB{head: head, tail: tail}
}

func (db LinkedListDB) first(key []byte) *linkedListNode {
	node := db.head.next

	for node != db.tail && string(node.item.Key) < string(key) {
		node = node.next
	}

	return node
}

func (db LinkedListDB) Get(key []byte) (value []byte, err error) {
	node := db.first(key)

	if node != db.tail && string(node.item.Key) == string(key) {
		return node.item.Value, nil
	}

	return nil, KeyError
}

func (db LinkedListDB) Has(key []byte) (ret bool, err error) {
	_, ok := db.Get(key)
	return ok == nil, nil
}

func (db LinkedListDB) Put(key, value []byte) error {
	node := db.first(key)

	if string(node.item.Key) == string(key) {
		node.item.Value = value
		return nil
	}

	currentNode := &linkedListNode{
		item: Item{Key: key, Value: value},
		next: node,
		prev: node.prev,
	}

	node.prev.next = currentNode
	node.prev = currentNode
	return nil
}

func (db LinkedListDB) Delete(key []byte) error {
	node := db.first(key)

	if node != db.tail && string(node.item.Key) == string(key) {
		node.prev.next = node.next
		node.next.prev = node.prev
		return nil
	}

	return KeyError
}

func (db LinkedListDB) RangeScan(start, limit []byte) (Iterator, error) {
	node := db.first(start)
	return &LinkedListIterator{db: &db, node: node, start: start, limit: limit}, nil
}

type LinkedListIterator struct {
	db           *LinkedListDB
	node         *linkedListNode
	start, limit []byte
}

func (iter *LinkedListIterator) Next() bool {
	if iter.node == iter.db.tail {
		return false
	}

	if len(iter.limit) > 0 && string(iter.node.item.Key) > string(iter.limit) {
		return false
	}

	iter.node = iter.node.next
	return true
}

func (iter *LinkedListIterator) Error() error {
	return nil
}

func (iter *LinkedListIterator) Key() []byte {
	return iter.node.item.Key
}

func (iter *LinkedListIterator) Value() []byte {
	return iter.node.item.Value
}

func (iter *LinkedListIterator) Flush(w io.Writer) {
	Flush(iter, w)
}
