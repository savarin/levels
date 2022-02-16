package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	path   = "/usr/share/dict/words"
	stride = 8
	begin  = "echo"
	end    = "server"
)

func loadWords() ([]string, error) {
	f, err := os.Open(path)
	defer f.Close()

	if err != nil {
		return nil, err
	}

	w := make([]string, 0)
	s := bufio.NewScanner(f)

	for s.Scan() {
		w = append(w, s.Text())
	}

	return w, nil
}

func runTest(words []string, db DB, name string) {
	fmt.Printf("%-20s", name)

	start := time.Now()
	for _, word := range words {
		db.Put([]byte(word), []byte(word))
	}
	fmt.Printf("%-20s", time.Since(start))

	start = time.Now()
	for i := 0; i < len(words); i += stride {
		db.Delete([]byte(words[i]))
	}
	fmt.Printf("%-20s", time.Since(start))

	start = time.Now()
	for _, word := range words {
		db.Get([]byte(word))
	}
	fmt.Printf("%-20s", time.Since(start))

	start = time.Now()
	r, _ := db.RangeScan([]byte(begin), []byte(end))
	for {
		r.Key()
		r.Value()

		hasNext := r.Next()
		if !hasNext {
			break
		}
	}
	fmt.Printf("%-20s", time.Since(start))
}

func main() {
	words, err := loadWords()

	if err != nil {
		log.Fatalf("Error: loading words\n")
	}

	store := make(map[string][]byte, 0)
	db := SimpleDB{store: store}

	fmt.Printf("%-20s%-20s%-20s%-20s%-20s\n", "name", "puts", "deletes", "gets", "rangescan")
	runTest(words, db, "simple")
	fmt.Printf("\n")
}
