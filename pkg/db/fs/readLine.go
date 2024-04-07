package fs

import (
	"encoding/csv"
	"errors"
	"io"
)

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
