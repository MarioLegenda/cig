package corrector

import (
	"errors"
	"github.com/MarioLegenda/cig/pkg/syntax/splitter"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCorrectorIsCorrect(t *testing.T) {
	sql := "SELECT * FROM path:../../../testdata/example.csv AS g WHERE 'g.Area' = 'A100100'"

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

	assert.Equal(t, 2, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidSelectChunk))
	assert.True(t, errors.Is(errs[1], InvalidFromChunk))
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
	sql := "SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'a' & 'b'"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidWhereClause))
}

func TestInCorrectorWhereClauseValue(t *testing.T) {
	sql := "SELECT      *      FROM path:../../../testdata/example.csv As g WHERE 'a' = 'b"

	errs := IsShallowSyntaxCorrect(splitter.NewSplitter(sql))

	assert.Equal(t, 1, len(errs))

	assert.True(t, errors.Is(errs[0], InvalidValueChuck))
}
