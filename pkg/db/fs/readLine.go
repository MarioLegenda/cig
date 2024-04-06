package fs

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

func ReadLine(f io.Reader) ([]byte, error) {
	br := bufio.NewReader(f)
	var buffer bytes.Buffer

	for {
		b, err := br.ReadByte()

		if b == 10 {
			break
		}

		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		if err != nil {
			return buffer.Bytes(), nil
		}

		buffer.WriteByte(b)
	}

	return buffer.Bytes(), nil
}
