package validation

import (
	"errors"
	"github.com/MarioLegenda/cig/internal/syntax/tokenizer"
	"github.com/MarioLegenda/cig/pkg"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInvalidSelectChunk(t *testing.T) {
	sql := "SEECT      *      FROM path:../../../testdata/example.csv AS g"

	_, err := ValidateAndCreateMetadata(tokenizer.Tokenize(sql))

	assert.NotNil(t, err)

	assert.True(t, errors.Is(err, pkg.InvalidSelectToken))
}

func TestInvalidSelectableColumns(t *testing.T) {
	statements := []string{
		"SELECT 'gYear'     FROM path:../../../testdata/example.csv As g",
		"SELECT 'gYear'      , 'g.Industry_aggregation_NZSIOC'      FROM path:../../../testdata/example.csv As g",
		"SELECT 'gYear'      , 'g.Industry_aggregation_NZSIOC'      FROM path:../../../testdata/example.csv As g",
		"SELECT 'g.Year      , 'gIndustry_aggregation_NZSIOC'      FROM path:../../../testdata/example.csv As g",
	}

	for _, s := range statements {
		_, err := ValidateAndCreateMetadata(tokenizer.Tokenize(s))

		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, pkg.InvalidSelectableColumns))
	}

	statements = []string{
		"SELECT 'g.Year'      ,     gIndustry_aggregation_NZSIOC      FROM path:../../../testdata/example.csv As g",
		"SELECT 'g.Year' 'g.Industry_aggregation_NZSIOC'      FROM path:../../../testdata/example.csv As g",
	}

	for _, s := range statements {
		_, err := ValidateAndCreateMetadata(tokenizer.Tokenize(s))

		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, pkg.InvalidFromToken))
	}

	invalidDuplicateColumn := "SELECT 'g.Year'     , 'g.Industry_aggregation_NZSIOC', 'g.Year'      FROM path:../../../testdata/example.csv As g"
	_, err := ValidateAndCreateMetadata(tokenizer.Tokenize(invalidDuplicateColumn))

	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, pkg.InvalidDuplicatedColumn))
}

func TestValidFrom(t *testing.T) {
	sql := "SElECT      *      FOM path:../../../testdata/example.csv AS g"

	_, err := ValidateAndCreateMetadata(tokenizer.Tokenize(sql))

	assert.NotNil(t, err)

	assert.True(t, errors.Is(err, pkg.InvalidFromToken))
}

func TestValidPath(t *testing.T) {
	statements := []string{
		"SELECT      *      FROM pth:../../../testdata/example.csv AS g",
		"SELECT      *      FROM path:../../testdata/example.csv AS g",
		"SELECT      *      FROM  AS g",
	}

	for _, s := range statements {
		_, err := ValidateAndCreateMetadata(tokenizer.Tokenize(s))

		assert.NotNil(t, err)

		assert.True(t, errors.Is(err, pkg.InvalidFilePathToken))
	}
}

func TestValidAsClause(t *testing.T) {
	sqlInvalidAsClause := "SELECT      *      FROM path:../../../testdata/example.csv A g"

	_, err := ValidateAndCreateMetadata(tokenizer.Tokenize(sqlInvalidAsClause))

	assert.NotNil(t, err)

	assert.True(t, errors.Is(err, pkg.InvalidAsToken))
}

func TestInvalidSelectableColumnAlias(t *testing.T) {
	statements := []string{
		"SELECT      'e.Zear'      FROM path:../../../testdata/example.csv As g",
	}

	for _, s := range statements {
		_, err := ValidateAndCreateMetadata(tokenizer.Tokenize(s))

		assert.NotNil(t, err)

		assert.True(t, errors.Is(err, pkg.InvalidColumnAlias))
	}
}

func TestValidWhereClause(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv As g WHER 'a' b"
	_, err := ValidateAndCreateMetadata(tokenizer.Tokenize(sql))

	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, pkg.InvalidWhereClause))
}

func TestValidConditions(t *testing.T) {
	statements := map[string]error{
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE a' = b":                             pkg.InvalidSelectableColumns,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'a.b' = b":                          pkg.InvalidConditionAlias,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' 56 b":                         pkg.InvalidComparisonOperator,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = b":                          pkg.InvalidValueToken,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = 'b' AND a' = b":             pkg.InvalidSelectableColumns,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = 'b' AND 'a.b' = b":          pkg.InvalidConditionAlias,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = 'b' AND 'g.b' 56 b":         pkg.InvalidComparisonOperator,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = 'b' AND 'g.b' = b":          pkg.InvalidValueToken,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = 'b' OR a' = b":              pkg.InvalidSelectableColumns,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = 'b' or 'a.b' = b":           pkg.InvalidConditionAlias,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = 'b' or 'g.b' 56 b":          pkg.InvalidComparisonOperator,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = 'b' Or 'g.b' = b":           pkg.InvalidValueToken,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b'::unknown = 'b' Or 'g.b' = b":  pkg.InvalidDataType,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = 'b' Or 'g.b'::unknown = b":  pkg.InvalidDataType,
		"SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.b' = 'b' ANd 'g.b'::unknown = b": pkg.InvalidDataType,
	}

	for sql, stmtErr := range statements {
		_, err := ValidateAndCreateMetadata(tokenizer.Tokenize(sql))

		assert.NotNil(t, err)
		assert.True(t, errors.Is(err, stmtErr))
	}
}

func TestValidMetadata(t *testing.T) {
	sql := `
SELECT   
    'e.ColumnOne','e.ColumnTwo'  , 'e.ColumnThree' ,'e.ColumnFour'     
	FROM path:../../../testdata/example.csv As e
	WHERE 'e.ColumnOne' = '1' 
	    AND 'e.ColumnTwo'::int = '5' 
	    
	    OR 'e.columnThree'::int = '6'
	    AND 'e.columnFour'::float = '6.6'
	    
	    `

	metadata, err := ValidateAndCreateMetadata(tokenizer.Tokenize(sql))

	assert.Nil(t, err)

	assert.Equal(t, len(metadata.SelectedColumns), 4)
	assert.Equal(t, metadata.Alias, "e")
	assert.Equal(t, metadata.FilePath, "../../../testdata/example.csv")
	assert.Equal(t, len(metadata.Conditions), 4)
}
