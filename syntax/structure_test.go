package syntax

import (
	"cig/syntax/syntaxParts"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStructureValid(t *testing.T) {
	sql := "SELECT * FROM path:../testdata/example.csv AS g WHERE g.Area = 'A100100'"

	res := NewStructure(sql)

	assert.Equal(t, false, res.HasErrors())
	assert.Equal(t, res.Result().Column().Type(), syntaxParts.ColumnType)
	assert.Equal(t, res.Result().FileDB().Type(), syntaxParts.FileDBType)
}
