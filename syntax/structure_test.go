package syntax

import (
	"cig/syntax/syntaxParts"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStructureValid(t *testing.T) {
	sql := "SELECT * FROM path:../testdata/example.csv AS g WHERE g.Area = 'A100100'"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Equal(t, res.Result().Column().Type(), syntaxParts.ColumnType)
	assert.Equal(t, res.Result().FileDB().Type(), syntaxParts.FileDBType)

	condition := res.Result().Condition()

	assert.Equal(t, condition.Column(), "g.Area")
	assert.Equal(t, condition.Value(), "'A100100'")
	assert.Equal(t, condition.Operator(), "=")

	assert.Nil(t, condition.Next())
	assert.Nil(t, condition.Prev())
}
