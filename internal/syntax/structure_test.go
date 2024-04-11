package syntax

import (
	"github.com/MarioLegenda/cig/internal/syntax/dataTypes"
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
	sql := "SELECT * FROM path:../../testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC'::int = 'Level 1' AND 'e.Year' != '2021' OR 'e.Industry_aggregation_NZSIOC'::float = 'Level 3' OR 'e.Variable_code'::string <= 'some value'"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Nil(t, res.Errors())

	head := res.Result().Condition()
	assert.Equal(t, head.Column().Original(), "'e.Industry_aggregation_NZSIOC'::int")
	assert.Equal(t, head.Column().Alias(), "e")
	assert.Equal(t, head.Column().Column(), "Industry_aggregation_NZSIOC")
	assert.Equal(t, head.Column().DataType(), dataTypes.Int)
	assert.Equal(t, head.Value().Original(), "'Level 1'")
	assert.Equal(t, head.Value().Value(), "Level 1")
	assert.Equal(t, head.Operator().Original(), "=")

	andOperator := head.Next()
	assert.Equal(t, andOperator.Operator().ConditionType(), operators.AndOperator)
	assert.Nil(t, andOperator.Value())
	assert.Nil(t, andOperator.Column())

	secondCondition := andOperator.Next()
	assert.Equal(t, secondCondition.Column().Original(), "'e.Year'")
	assert.Equal(t, secondCondition.Column().Alias(), "e")
	assert.Equal(t, secondCondition.Column().Column(), "Year")
	assert.Equal(t, secondCondition.Value().Original(), "'2021'")
	assert.Equal(t, secondCondition.Value().Value(), "2021")
	assert.Equal(t, secondCondition.Operator().Original(), "!=")

	orOperator := secondCondition.Next()
	assert.Equal(t, orOperator.Operator().ConditionType(), operators.OrOperator)
	assert.Nil(t, orOperator.Value())
	assert.Nil(t, orOperator.Column())

	thirdCondition := orOperator.Next()
	assert.Equal(t, thirdCondition.Column().Original(), "'e.Industry_aggregation_NZSIOC'::float")
	assert.Equal(t, thirdCondition.Column().Alias(), "e")
	assert.Equal(t, thirdCondition.Column().DataType(), dataTypes.Float)
	assert.Equal(t, thirdCondition.Column().Column(), "Industry_aggregation_NZSIOC")
	assert.Equal(t, thirdCondition.Value().Original(), "'Level 3'")
	assert.Equal(t, thirdCondition.Value().Value(), "Level 3")
	assert.Equal(t, thirdCondition.Operator().Original(), "=")

	orOperator = thirdCondition.Next()
	assert.Equal(t, orOperator.Operator().ConditionType(), operators.OrOperator)
	assert.Nil(t, orOperator.Value())
	assert.Nil(t, orOperator.Column())

	fourthCondition := orOperator.Next()
	assert.Equal(t, fourthCondition.Column().Original(), "'e.Variable_code'::string")
	assert.Equal(t, fourthCondition.Column().Alias(), "e")
	assert.Equal(t, fourthCondition.Column().DataType(), dataTypes.String)
	assert.Equal(t, fourthCondition.Column().Column(), "Variable_code")
	assert.Equal(t, fourthCondition.Value().Original(), "'some value'")
	assert.Equal(t, fourthCondition.Value().Value(), "some value")
	assert.Equal(t, fourthCondition.Operator().Original(), "<=")
}
