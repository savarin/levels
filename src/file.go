package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	blockSize = 1 << 12
)

type sparseIndexEntry struct {
	key    []byte
	offset uint32
}

type simpleWriter struct {
	Offset uint32
	Writer io.Writer
}

func (w *simpleWriter) Write(b []byte) error {
	n, err := w.Writer.Write(b)

	if err != nil {
		return err
	}

	w.Offset += uint32(n)
	return nil
}

func (w *simpleWriter) WriteLen(n uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], n)
	return w.Write(buf[:])
}

func Flush(db DB, w io.Writer) error {
	writer := simpleWriter{
		Writer: w,
	}

	iter, _ := db.RangeScan([]byte{}, []byte{})

	var sparseIndex []sparseIndexEntry
	var nextCheckpoint uint32

	var key, value []byte
	var finalKey []byte
	var finalOffset uint32

	for {
		startOffset := writer.Offset

		key = iter.Key()
		value = iter.Value()
		finalOffset = startOffset

		if nextCheckpoint <= startOffset {
			e := sparseIndexEntry{
				key:    key,
				offset: startOffset,
			}

			sparseIndex = append(sparseIndex, e)
			nextCheckpoint = startOffset + uint32(blockSize)

			finalKey = key
		}

		err := writer.WriteLen(uint32(len(key)))
		if err != nil {
			return fmt.Errorf("writing length (%d) of key %q in table: %w", len(key), key, err)
		}

		err = writer.Write(key)
		if err != nil {
			return fmt.Errorf("writing key %q: %w in table", key, err)
		}

		err = writer.WriteLen(uint32(len(value)))
		if err != nil {
			return fmt.Errorf("writing length (%d) of value %q in table: %w", len(value), value, err)
		}

		err = writer.Write(value)
		if err != nil {
			return fmt.Errorf("writing value %q in table: %w", value, err)
		}

		hasNext := iter.Next()

		if !hasNext {
			break
		}
	}

	if string(key) != string(finalKey) {
		e := sparseIndexEntry{
			key:    key,
			offset: finalOffset,
		}

		sparseIndex = append(sparseIndex, e)
	}

	sparseIndexOffset := writer.Offset

	for _, e := range sparseIndex {
		indexKey := e.key
		indexOffset := e.offset

		err := writer.WriteLen(uint32(len(indexKey)))
		if err != nil {
			return fmt.Errorf("writing length (%d) of key in sparse index%q: %w", len(indexKey), indexKey, err)
		}

		err = writer.Write(indexKey)
		if err != nil {
			return fmt.Errorf("writing key %q in sparse index: %w", indexKey, err)
		}

		err = writer.WriteLen(indexOffset)
		if err != nil {
			return fmt.Errorf("writing value %q in sparse index: %w", indexOffset, err)
		}
	}

	err := writer.WriteLen(sparseIndexOffset)
	if err != nil {
		return fmt.Errorf("writing starting offset in sparse index: %s", err)
	}

	return nil
}
