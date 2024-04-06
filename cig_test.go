package cig

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testStruct struct {
}

func TestCig(t *testing.T) {
	cig := New()

	cig.Run("SELECT * FROM path:testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1'")
}

func TestShouldCloseCigWithoutErrors(t *testing.T) {
	cig := New()

	cig.Run("SELECT * FROM path:testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1'")

	closeRes := cig.Close()

	assert.Nil(t, closeRes.Errors())
}
