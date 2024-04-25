package fs

import (
	"encoding/csv"
	"errors"
	"io"
)

func NewLineReader(f io.Reader) func() ([]string, error) {
	r := csv.NewReader(f)

	return func() ([]string, error) {
		b, err := r.Read()
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		return b, nil
	}
}
