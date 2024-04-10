package conditionResolver

import (
	"github.com/MarioLegenda/cig/internal/syntax"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConditionResolver(t *testing.T) {
	sql := "SELECT * FROM path:../../../testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1' OR 'e.Industry_aggregation_NZSIOC' = 'Level 2'"

	structure := syntax.NewStructure(sql)

	assert.Nil(t, structure.Errors())
	assert.NotNil(t, structure.Result().Condition())

	cm := NewColumnMetadata(
		[]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		[]string{
			"Year",
			"Industry_aggregation_NZSIOC",
			"Industry_code_NZSIOC",
			"Industry_name_NZSIOC",
			"Units",
			"Variable_code",
			"Variable_name",
			"Variable_category",
			"Value",
			"Industry_code_ANZSIC06",
		},
	)

	lines := []string{
		"2021",
		"Level 2",
		"99999",
		"All industries",
		"Dollars (millions)",
		"H01",
		"Total income",
		"Financial performance",
		"757,504",
		"ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)",
	}

	resolved, err := ResolveCondition(structure.Result().Condition(), cm, lines)

	assert.Nil(t, err)
	assert.True(t, resolved)
}
