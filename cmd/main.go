package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	f, err := os.Open("../testdata/example.csv")
	if err != nil {
		log.Fatal(err)
	}

	lineReader1 := NewLineReader(f, false)
	fmt.Println(lineReader1())
	fmt.Println(lineReader1())
	fmt.Println(lineReader1())
	fmt.Println(lineReader1())
	fmt.Println(lineReader1())
}

func NewLineReader(f io.Reader, skipColumns bool) func() ([]string, error) {
	r := csv.NewReader(f)

	return func() ([]string, error) {
		b, err := r.Read()
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		if skipColumns {
			b, err := r.Read()
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, err
			}

			return b, nil
		}

		return b, nil
	}
}
