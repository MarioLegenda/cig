package splitter

import (
	"fmt"
	"strings"
)

const Separator = "#"

type splitter struct {
	chunks []string
}

type Splitter interface {
	Chunks() []string
}

func (s splitter) Chunks() []string {
	return s.chunks
}

func NewSplitter(sql string) Splitter {
	sql = removeWhitespace(sql)
	s := strings.Split(sql, Separator)

	return splitter{chunks: s}
}

func removeWhitespace(s string) string {
	sql := []byte(s)
	base := ""

	whitespaceMode := false
	quoteMode := false
	for i := 0; i < len(sql); i++ {
		b := sql[i]

		if b == 39 && !quoteMode {
			whitespaceMode = false
			quoteMode = true
			base += string(b)
			continue
		}

		// 39 is quote
		if b != 39 && quoteMode {
			base += string(b)
			continue
		}

		if b == 39 && quoteMode {
			quoteMode = false
			base += string(b)
			continue
		}

		// 32 is whitespace
		if (b == 32 || b == 10 || b == 9) && !whitespaceMode {
			whitespaceMode = true
			base += Separator
			continue
		}

		if (b != 32 || b != 10 || b != 9) && whitespaceMode {
			whitespaceMode = false
		}

		if !whitespaceMode {
			base += string(b)
		}
	}

	fmt.Println(base)

	return base
}
