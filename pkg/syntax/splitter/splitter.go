package splitter

import (
	"regexp"
	"strings"
)

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
	m1 := regexp.MustCompile(`\s+`)
	sql = m1.ReplaceAllString(sql, " ")

	s := strings.Split(sql, " ")

	return splitter{chunks: s}
}
