package splitter

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestSplitterWithValidSpaces(t *testing.T) {
	sql := "SELECT * FROM path:testdata/example.csv AS g WHERE g.Area = A100100"
	s := NewSplitter(sql)

	assert.Equal(t, len(s.Chunks()), 10)
	assert.Equal(t, strings.Join(s.Chunks(), " "), sql)
}

func TestSplitterWithInValidSpaces(t *testing.T) {
	sql := "SELECT     *     FROM     path:testdata/example.csv      WHERE          g.Area =            A100100"
	s := NewSplitter(sql)

	assert.Equal(t, len(s.Chunks()), 8)
}
