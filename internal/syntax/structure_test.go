package syntax

import (
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStructureValid(t *testing.T) {
	sql := "SELECT * FROM path:../../testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1'"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Nil(t, res.Errors())

	condition := res.Result().Condition()

	assert.Equal(t, condition.Column().Original(), "'e.Industry_aggregation_NZSIOC'")
	assert.Equal(t, condition.Column().Alias(), "e")
	assert.Equal(t, condition.Column().Column(), "Industry_aggregation_NZSIOC")

	assert.Equal(t, condition.Value().Original(), "'Level 1'")
	assert.Equal(t, condition.Value().Value(), "Level 1")

	assert.Equal(t, condition.Operator().Original(), "=")

	assert.Nil(t, condition.Next())
	assert.Nil(t, condition.Prev())
}

func TestStructureWithMultipleConditions(t *testing.T) {
	sql := "SELECT * FROM path:../../testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1' AND 'e.Industry_aggregation_NZSIOC' != 'Level 2' OR 'e.Industry_aggregation_NZSIOC' = 'Level 3'"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Nil(t, res.Errors())

	head := res.Result().Condition()
	assert.Equal(t, head.Column().Original(), "'e.Industry_aggregation_NZSIOC'")
	assert.Equal(t, head.Column().Alias(), "e")
	assert.Equal(t, head.Column().Column(), "Industry_aggregation_NZSIOC")
	assert.Equal(t, head.Value().Original(), "'Level 1'")
	assert.Equal(t, head.Value().Value(), "Level 1")
	assert.Equal(t, head.Operator().Original(), "=")

	andOperator := head.Next()
	assert.Equal(t, andOperator.Operator().ConditionType(), operators.AndOperator)
	assert.Nil(t, andOperator.Value())
	assert.Nil(t, andOperator.Column())

	secondCondition := andOperator.Next()
	assert.Equal(t, secondCondition.Column().Original(), "'e.Industry_aggregation_NZSIOC'")
	assert.Equal(t, secondCondition.Column().Alias(), "e")
	assert.Equal(t, secondCondition.Column().Column(), "Industry_aggregation_NZSIOC")
	assert.Equal(t, secondCondition.Value().Original(), "'Level 2'")
	assert.Equal(t, secondCondition.Value().Value(), "Level 2")
	assert.Equal(t, secondCondition.Operator().Original(), "!=")

	orOperator := secondCondition.Next()
	assert.Equal(t, orOperator.Operator().ConditionType(), operators.OrOperator)
	assert.Nil(t, orOperator.Value())
	assert.Nil(t, orOperator.Column())
}
