package syntax

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStructureValid(t *testing.T) {
	sql := "SELECT * FROM path:../testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1'"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Nil(t, res.Errors())

	condition := res.Result().Condition()

	assert.Equal(t, condition.Column(), "'e.Industry_aggregation_NZSIOC'")
	assert.Equal(t, condition.Value(), "'Level 1'")
	assert.Equal(t, condition.Operator(), "=")

	assert.Nil(t, condition.Next())
	assert.Nil(t, condition.Prev())
}
