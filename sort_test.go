package cig

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSingleColumnStringSort(t *testing.T) {
	c := New()

	sql := "SELECT 'e.Year' FROM path:testdata/example.csv AS e ORDER BY 'e.Year'  LIMIT 10 "

	res := c.Run(sql)
	assert.Nil(t, res.Error)

	foundResults := res.Data

	assert.Equal(t, 10, len(foundResults))

	for _, res := range foundResults {
		assert.Equal(t, res["Year"], "2013")
	}

	sql = "SELECT 'e.Year' FROM path:testdata/example.csv AS e ORDER BY 'e.Year' DESC  LIMIT 10 "

	res = c.Run(sql)
	assert.Nil(t, res.Error)

	foundResults = res.Data

	assert.Equal(t, 10, len(foundResults))

	for _, res := range foundResults {
		assert.Equal(t, res["Year"], "2021")
	}
}
