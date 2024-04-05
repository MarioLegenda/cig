package pkg

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testStruct struct {
}

func TestCig(t *testing.T) {
	cig := New()

	_, err := cig.Run("SELECT * FROM path:testdata/example.csv AS e WHERE e.Industry_aggregation_NZSIOC = 'Level 1'")

	assert.Nil(t, err)
}
