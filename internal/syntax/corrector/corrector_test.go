package corrector

import (
	"errors"
	"github.com/MarioLegenda/cig/internal/syntax/splitter"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCorrectorIsCorrect(t *testing.T) {
	sql := "SELECT * FROM path:../../../testdata/example.csv AS g WHERE 'g.Area' = 'A100100' AND 'g.Locale' != '45' OR 'g.Field' <= '23'"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 0, len(errs))
}

func TestCorrectorMinChunks(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv AS"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))
}

func TestCorrectorInvalidSelectChunk(t *testing.T) {
	sql := "SEECT      *      FROM path:../../../testdata/example.csv AS g"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidSelectChunk))
}

func TestCorrectorInvalidSelectAndFromChunk(t *testing.T) {
	sql := "SEECT      *      FOM path:../../../testdata/example.csv AS g"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidNumberOfChunks))
}

func TestCorrectorInvalidPathChunk(t *testing.T) {
	sql := "SELECT      *      FROM pth:../../../testdata/example.csv AS g"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidFilePathChunk))
}

func TestCorrectorInvalidFileNotExists(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/ge.csv AS g"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidFilePath))
}

func TestCorrectorInvalidAsChuck(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv A g"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidAsChunk))
}

func TestInCorrectorWhereClause(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'a' b"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidWhereClause))
}

func TestInCorrectorWhereClauseOperator(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.a' & 'g.b'"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidWhereClause))
}

func TestInCorrectorWhereClauseValue(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.a' = 'b"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidValueChuck))
}

func TestCorrectorMultipleInvalidConditions(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.a' = 'b' AD 'g.b' != 'a' OT 'g.c' != 'o' BSK 'g.C' <= 'O'"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 3, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidWhereClause))
	assert.True(t, errors.Is(errs[1], InvalidWhereClause))
	assert.True(t, errors.Is(errs[2], InvalidWhereClause))
}

func TestCorrectorMultipleValidConditions(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.a' = 'b' AND 'g.b' != 'a' OR 'g.c' != 'o' AND 'g.C' <= 'O'"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 0, len(errs))
}

func TestIncorrectDataType(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.a'::unrecognized = 'b' AND 'g.b'::unrecognized != 'a' OR 'g.c'::unrecognized != 'o' AND 'g.C'::unrecognized <= 'O'"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 4, len(errs))

	for _, e := range errs {
		assert.True(t, errors.Is(e, InvalidDataType))
	}
}

func TestValidateDataTypes(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'g.a'::int = 'b' AND 'g.b'::float != 'a' OR 'g.c'::int != 'o' AND 'g.C'::float <= 'O'"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 0, len(errs))
}

func TestValidateColumnAlias(t *testing.T) {
	sql := "SELECT      'g.Year',         'e.Industry_aggregation_NZSIOC',         'z.Industry_code_NZSIOC'      FROM path:../../../testdata/example.csv As g WHERE 'g.a'::int = 'b' AND 'g.b'::float != 'a' OR 'g.c'::int != 'o' AND 'g.C'::float <= 'O'"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 2, len(errs))
}

func TestValidAliasInConditions(t *testing.T) {
	sql := "SELECT      'g.Year',         'g.Industry_aggregation_NZSIOC',         'g.Industry_code_NZSIOC'      FROM path:../../../testdata/example.csv As g WHERE 'z.a'::int = 'b' AND 'z.b'::float != 'a' OR 'z.c'::int != 'o' AND 'z.C'::float <= 'O'"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 4, len(errs))

	for _, k := range errs {
		assert.True(t, errors.Is(k, InvalidConditionAlias))
	}
}
