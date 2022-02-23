package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Table struct {
	reader      ReaderSeeker
	sparseIndex []sparseIndexEntry
}

func Open(r ReaderSeeker) (*Table, error) {
	indexEnd, err := r.Seek(-4, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seeking to end of file to read index start location: %s", err)
	}

	var s uint32
	err = binary.Read(r, binary.LittleEndian, &s)
	if err != nil {
		return nil, fmt.Errorf("reading index start location: %s", err)
	}
	indexStart := int64(s)

	if indexStart > indexEnd {
		return nil, fmt.Errorf("corrupted table file: index end %d > index start %d", indexEnd, indexStart)
	}

	if indexStart == indexEnd {
		return &Table{
			reader: r,
		}, nil
	}

	_, err = r.Seek(int64(indexStart), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to start of index: %s", err)
	}

	indexReader := io.NewSectionReader(r, indexStart, indexEnd-indexStart)
	var sparseIndex []sparseIndexEntry

	for {
		var keyLength uint32
		err := binary.Read(indexReader, binary.LittleEndian, &keyLength)
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, fmt.Errorf("reading key length in index: %s", err)
		}

		key := make([]byte, keyLength)
		_, err = indexReader.Read(key)
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("corrupted index: EOF while reading key")
			}

			return nil, fmt.Errorf("reading key: %s", err)
		}

		var blockOffset uint32
		err = binary.Read(indexReader, binary.LittleEndian, &blockOffset)
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("corrupted index: EOF while reading offset")
			}

			return nil, fmt.Errorf("reading offset for key %s: %s", key, err)
		}

		e := sparseIndexEntry{
			key:    key,
			offset: blockOffset,
		}
		sparseIndex = append(sparseIndex, e)
	}

	return &Table{
		reader:      r,
		sparseIndex: sparseIndex,
	}, nil
}

func (t Table) getBlock(key []byte) (offsetStart, offsetEnd uint32, isOffset bool) {
	for i := 1; i < len(t.sparseIndex); i++ {
		endKey := t.sparseIndex[i].key

		if string(key) < string(endKey) {
			return t.sparseIndex[i-1].offset, t.sparseIndex[i].offset, true
		}
	}

	return 0, 0, false
}

func (t Table) findKey(offsetStart, offsetEnd uint32, key []byte) (value []byte, err error) {
	reader := io.NewSectionReader(t.reader, int64(offsetStart), int64(offsetEnd-offsetStart))

	for {
		var keyLength uint32
		err = binary.Read(reader, binary.LittleEndian, &keyLength)
		if err != nil {
			if err == io.EOF {
				return nil, KeyError
			}

			return nil, fmt.Errorf("reading key length: %s", err)
		}

		currentKey := make([]byte, keyLength)
		_, err = reader.Read(currentKey)
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("corrupted block: end of block while reading key")
			}

			return nil, fmt.Errorf("reading key: %s", err)
		}

		if string(key) != string(currentKey) {
			continue
		}

		var valueLength uint32
		err = binary.Read(reader, binary.LittleEndian, &valueLength)
		if err != nil {
			if err == io.EOF {
				return nil, ValueError
			}

			return nil, fmt.Errorf("reading value length: %s", err)
		}

		value := make([]byte, valueLength)
		_, err = reader.Read(value)
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("corrupted block: end of block while reading value")
			}

			return nil, fmt.Errorf("reading value: %s", err)
		}

		return value, nil
	}
}
