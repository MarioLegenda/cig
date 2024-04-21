package syntax

import (
	"github.com/MarioLegenda/cig/internal/syntax/dataTypes"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStructureValid(t *testing.T) {
	sql := "SELECT * FROM path:../../testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC'::string = 'Level 1'"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Nil(t, res.Errors())

	condition := res.Result().Condition()

	assert.Equal(t, condition.Column().Alias(), "e")
	assert.Equal(t, condition.Column().Column(), "Industry_aggregation_NZSIOC")
	assert.Equal(t, condition.Column().DataType(), dataTypes.String)

	assert.Equal(t, condition.Value().Value(), "Level 1")

	assert.Nil(t, condition.Next())
	assert.Nil(t, condition.Prev())
}

func TestStructureWithMultipleConditions(t *testing.T) {
	sql := "SELECT 'e.Industry_aggregation_NZSIOC','e.Year' FROM path:../../testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1' AND 'e.Year'::int != '2021' OR 'e.Industry_aggregation_NZSIOC' = 'Level 3' OR 'e.Variable_code'::string <= 'some value'"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Nil(t, res.Errors())

	assert.True(t, res.Result().Column().HasColumn("Industry_aggregation_NZSIOC"))
	assert.True(t, res.Result().Column().HasColumn("Year"))
	assert.Equal(t, len(res.Result().Column().Columns()), 2)

	head := res.Result().Condition()
	assert.Equal(t, head.Column().Alias(), "e")
	assert.Equal(t, head.Column().Column(), "Industry_aggregation_NZSIOC")
	assert.Equal(t, head.Column().DataType(), "")
	assert.Equal(t, head.Value().Value(), "Level 1")

	andOperator := head.Next()

	assert.Equal(t, andOperator.Operator().ConditionType(), operators.AndOperator)
	assert.Nil(t, andOperator.Value())
	assert.Nil(t, andOperator.Column())

	secondCondition := andOperator.Next()
	assert.Equal(t, secondCondition.Column().Alias(), "e")
	assert.Equal(t, secondCondition.Column().DataType(), dataTypes.Int)
	assert.Equal(t, secondCondition.Column().Column(), "Year")
	assert.Equal(t, secondCondition.Value().Value(), "2021")

	orOperator := secondCondition.Next()
	assert.Equal(t, orOperator.Operator().ConditionType(), operators.OrOperator)
	assert.Nil(t, orOperator.Value())
	assert.Nil(t, orOperator.Column())

	thirdCondition := orOperator.Next()
	assert.Equal(t, thirdCondition.Column().Alias(), "e")
	assert.Equal(t, thirdCondition.Column().DataType(), "")
	assert.Equal(t, thirdCondition.Column().Column(), "Industry_aggregation_NZSIOC")
	assert.Equal(t, thirdCondition.Value().Value(), "Level 3")

	orOperator = thirdCondition.Next()
	assert.Equal(t, orOperator.Operator().ConditionType(), operators.OrOperator)
	assert.Nil(t, orOperator.Value())
	assert.Nil(t, orOperator.Column())

	fourthCondition := orOperator.Next()
	assert.Equal(t, fourthCondition.Column().Alias(), "e")
	assert.Equal(t, fourthCondition.Column().DataType(), dataTypes.String)
	assert.Equal(t, fourthCondition.Column().Column(), "Variable_code")
	assert.Equal(t, fourthCondition.Value().Value(), "some value")
}

func TestLimitConstraintValid(t *testing.T) {
	t.Skip("")

	sql := "SELECT * FROM path:../../testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC'::string = 'Level 1' LIMIT 10"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Nil(t, res.Errors())

	assert.NotNil(t, res.Result().Constraints())
	assert.NotNil(t, res.Result().Constraints().Limit())
	assert.Equal(t, res.Result().Constraints().Limit().Value(), int64(10))
	assert.Nil(t, res.Result().Constraints().Offset())

	condition := res.Result().Condition()

	assert.Equal(t, condition.Column().Alias(), "e")
	assert.Equal(t, condition.Column().Column(), "Industry_aggregation_NZSIOC")
	assert.Equal(t, condition.Column().DataType(), dataTypes.String)

	assert.Equal(t, condition.Value().Value(), "Level 1")

	assert.Nil(t, condition.Next())
	assert.Nil(t, condition.Prev())
}

func TestOffsetConstraintValid(t *testing.T) {
	t.Skip("")

	sql := "SELECT * FROM path:../../testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC'::string = 'Level 1' Offset 10"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Nil(t, res.Errors())

	assert.NotNil(t, res.Result().Constraints())
	assert.Nil(t, res.Result().Constraints().Limit())
	assert.NotNil(t, res.Result().Constraints().Offset())
	assert.Equal(t, res.Result().Constraints().Offset().Value(), int64(10))

	condition := res.Result().Condition()

	assert.Equal(t, condition.Column().Alias(), "e")
	assert.Equal(t, condition.Column().Column(), "Industry_aggregation_NZSIOC")
	assert.Equal(t, condition.Column().DataType(), dataTypes.String)

	assert.Equal(t, condition.Value().Value(), "Level 1")

	assert.Nil(t, condition.Next())
	assert.Nil(t, condition.Prev())
}

func TestAllConstraintValid(t *testing.T) {
	t.Skip("")

	sql := "SELECT * FROM path:../../testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC'::string = 'Level 1' Offset 10 LIMIT 45"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Nil(t, res.Errors())

	assert.NotNil(t, res.Result().Constraints())
	assert.NotNil(t, res.Result().Constraints().Limit())
	assert.NotNil(t, res.Result().Constraints().Offset())
	assert.Equal(t, res.Result().Constraints().Offset().Value(), int64(10))
	assert.Equal(t, res.Result().Constraints().Limit().Value(), int64(45))

	condition := res.Result().Condition()

	assert.Equal(t, condition.Column().Alias(), "e")
	assert.Equal(t, condition.Column().Column(), "Industry_aggregation_NZSIOC")
	assert.Equal(t, condition.Column().DataType(), dataTypes.String)

	assert.Equal(t, condition.Value().Value(), "Level 1")

	assert.Nil(t, condition.Next())
	assert.Nil(t, condition.Prev())
}
