package main

import (
	"encoding/csv"
	"errors"
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
	sql := "SELECT      'g.Year',         'e.Industry_aggregation_NZSIOC',         'z.Industry_code_NZSIOC'      FROM path:../../../testdata/example.csv As g WHERE 'a'::int = 'b' AND 'b'::float != 'a' OR 'c'::int != 'o' AND 'C'::float <= 'O'"

	c := cig.New()

	c.Run(sql)
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
