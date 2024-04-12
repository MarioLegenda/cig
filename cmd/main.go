package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/MarioLegenda/cig"
	"io"
)

func main() {
	tryCig()
	/*	f, err := os.Open("../testdata/example.csv")
		if err != nil {
			log.Fatal(err)
		}

		lineReader1 := NewLineReader(f, false)
		fmt.Println(lineReader1())
		fmt.Println(lineReader1())
		fmt.Println(lineReader1())
		fmt.Println(lineReader1())
		fmt.Println(lineReader1())*/
}

func tryCig() {
	c := cig.New()

	result := c.Run("SELECT 'e.Year' FROM path:../testdata/example.csv AS e WHERE 'e.Year'::int > '2013'")

	fmt.Println(result.Errors())

	fmt.Println(result.Result())
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
