package job

import (
	"github.com/MarioLegenda/cig/internal/db/conditionResolver"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
	"sort"
	"strconv"
)

type sortResult [][]string

var currentPosition int
var columns []string
var direction string

func (s sortResult) Len() int {
	return len(s)
}

func (s sortResult) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortResult) Less(i, j int) bool {
	v1, _ := strconv.ParseInt(s[i][currentPosition], 10, 64)
	v2, _ := strconv.ParseInt(s[j][currentPosition], 10, 64)

	if direction == operators.Asc {
		return v1 < v2
	}

	return v1 > v2
}

func sortResults(result [][]string, orderBy syntaxStructure.OrderBy, metadata conditionResolver.ColumnMetadata) [][]string {
	direction = operators.Asc
	if orderBy.Direction() == operators.Desc {
		direction = orderBy.Direction()
	}

	orderByColumns := orderBy.Columns()
	for _, c := range orderByColumns {
		currentPosition = metadata.Position(c.Column())
		sort.Sort(sortResult(result))
	}

	return result
}
