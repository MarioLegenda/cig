package pkg

import (
	"testing"
)

type testStruct struct {
}

func TestCig(t *testing.T) {
	cig := New()

	cig.Run("SELECT * FROM path:testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1'")
}
