package main

import (
	"encoding/binary"
	"io"
)

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

func Write(iter Iterator, w io.Writer) {
	writer := simpleWriter{
		Writer: w,
	}

	for {
		key := iter.Key()
		value := iter.Value()

		writer.WriteLen(uint32(len(key)))
		writer.Write(key)
		writer.WriteLen(uint32(len(value)))
		writer.Write(value)

		hasNext := iter.Next()

		if !hasNext {
			break
		}
	}
}
