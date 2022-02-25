package main

import (
	"io"
	"math/rand"
)

const (
	maxLevel = 12
)

type skipListNode struct {
	item  Item
	next  [maxLevel]*skipListNode
	level int
}

type SkipListDB struct {
	head   *skipListNode
	levels int
}

func NewSkipListDB() *SkipListDB {
	head := &skipListNode{level: maxLevel}
	return &SkipListDB{head: head, levels: 1}
}

func (db *SkipListDB) findPrevious(key []byte) [maxLevel]*skipListNode {
	var result [maxLevel]*skipListNode

	node := db.head
	for i := db.levels - 1; i >= 0; i-- {
		for node.next[i] != nil {
			if string(node.next[i].item.Key) < string(key) {
				node = node.next[i]
			} else {
				break
			}
		}

		result[i] = node
	}

	return result
}

func verifyNode(previous [maxLevel]*skipListNode, key []byte) *skipListNode {
	if previous[0].next[0] != nil && string(previous[0].next[0].item.Key) == string(key) {
		return previous[0].next[0]
	}

	return nil
}

func generateLevel() int {
	level := 1
	for level < maxLevel && rand.Intn(2) == 1 {
		level++
	}

	return level
}

func (db *SkipListDB) Get(key []byte) (value []byte, err error) {
	previous := db.findPrevious(key)
	node := verifyNode(previous, key)

	if node != nil {
		return node.item.Value, nil
	}

	return nil, KeyError
}

func (db *SkipListDB) Has(key []byte) (ret bool, err error) {
	_, ok := db.Get(key)
	return ok == nil, nil
}

func (db *SkipListDB) Put(key, value []byte) error {
	previous := db.findPrevious(key)
	node := verifyNode(previous, key)

	if node != nil {
		node.item.Value = value
		return nil
	}

	level := generateLevel()
	node = &skipListNode{
		item:  Item{Key: key, Value: value},
		level: level,
	}

	if level > db.levels {
		for i := level - 1; i >= db.levels; i-- {
			db.head.next[i] = node
		}
		for i := db.levels - 1; i >= 0; i-- {
			node.next[i] = previous[i].next[i]
			previous[i].next[i] = node
		}
		db.levels = level
	} else {
		for i := level - 1; i >= 0; i-- {
			node.next[i] = previous[i].next[i]
			previous[i].next[i] = node
		}
	}

	return nil
}

func (db *SkipListDB) Delete(key []byte) error {
	previous := db.findPrevious(key)
	node := verifyNode(previous, key)

	if node != nil {
		for i := node.level - 1; i >= 0; i-- {
			previous[i].next[i] = node.next[i]
		}

		return nil
	}

	return KeyError
}

func (db *SkipListDB) RangeScan(start, limit []byte) (Iterator, error) {
	previous := db.findPrevious(start)
	node := previous[0].next[0]
	return &SkipListIterator{db: db, node: node, start: start, limit: limit}, nil
}

func (db *SkipListDB) Flush(w io.Writer) error {
	return Flush(db, w)
}

type SkipListIterator struct {
	db           *SkipListDB
	node         *skipListNode
	start, limit []byte
}

func (iter *SkipListIterator) Next() bool {
	if iter.node.next[0] == nil {
		return false
	}

	if len(iter.limit) > 0 && string(iter.node.item.Key) > string(iter.limit) {
		return false
	}

	iter.node = iter.node.next[0]
	return true
}

func (iter *SkipListIterator) Error() error {
	return nil
}

func (iter *SkipListIterator) Key() []byte {
	return iter.node.item.Key
}

func (iter *SkipListIterator) Value() []byte {
	return iter.node.item.Value
}
