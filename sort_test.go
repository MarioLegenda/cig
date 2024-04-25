package cig

import (
	"encoding/csv"
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"sort"
	"testing"
)

func TestSingleColumnStringSort(t *testing.T) {
	c := New()

	sql := "SELECT 'e.Industry_aggregation_NZSIOC' FROM path:testdata/example.csv AS e     ORDER BY 'e.Industry_aggregation_NZSIOC'  LIMIT 10 "

	res := c.Run(sql)

	assert.False(t, res.HasErrors())
	assert.Equal(t, 0, len(res.Errors()))

	foundResults := res.Result()

	assert.Equal(t, 10, len(foundResults))

	cls, err := collectColumn(1)
	assert.Nil(t, err)

	sort.Strings(cls)

	cigCls := make([]string, len(foundResults))
	for i, c := range foundResults {
		cigCls[i] = c["Industry_aggregation_NZSIOC"]
	}

	assert.Equal(t, len(cigCls), len(cls))

	for i, fileColumn := range cls {
		assert.Equal(t, cigCls[i], fileColumn)
	}
}

func collectColumn(pos int) ([]string, error) {
	f, err := os.Open("testdata/example.csv")
	if err != nil {
		return nil, err
	}

	columns := make([]string, 0)
	r := csv.NewReader(f)
	defer f.Close()

	for {
		b, err := r.Read()
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		if errors.Is(err, io.EOF) {
			return columns, nil
		}

		columns = append(columns, b[1])
	}
}
