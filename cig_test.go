package cig

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testStruct struct {
}

func TestGettingAllResults(t *testing.T) {
	cig := New()

	res := cig.Run("SELECT * FROM path:testdata/example.csv AS e")

	assert.False(t, res.HasErrors())
	assert.Equal(t, 0, len(res.Errors()))

	foundResults := res.Result()

	assert.Equal(t, 20858, len(foundResults))

	cig.Close()
}

func TestGettingResultsWithSingleWhereClause(t *testing.T) {
	cig := New()

	res := cig.Run("SELECT * FROM path:testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1'")

	assert.False(t, res.HasErrors())
	assert.Equal(t, 0, len(res.Errors()))

	foundResults := res.Result()

	assert.Equal(t, 2511, len(foundResults))

	cig.Close()
}

func TestShouldCloseCigWithoutErrors(t *testing.T) {
	cig := New()

	cig.Run("SELECT * FROM path:testdata/example.csv AS e")
	closeRes := cig.Close()

	assert.Nil(t, closeRes.Errors())
}
