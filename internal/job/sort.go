package job

import (
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
	"sort"
)

type MapResult map[string]string

var currentColumn int
var columns []string
var direction string

func (s SearchResult) Len() int {
	return len(s)
}

func (s SearchResult) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SearchResult) Less(i, j int) bool {
	if direction == operators.Asc {
		return s[i][columns[currentColumn]] < s[j][columns[currentColumn]]
	}

	return s[i][columns[currentColumn]] > s[j][columns[currentColumn]]
}

func sortResults(result SearchResult, orderBy syntaxStructure.OrderBy) SearchResult {
	currentColumn = 0
	columns = make([]string, 0)
	direction = orderBy.Direction()
	if direction == "" {
		direction = operators.Asc
	}

	ssColumns := orderBy.Columns()
	for _, c := range ssColumns {
		columns = append(columns, c.Column())
	}

	for i, _ := range columns {
		currentColumn = i
		sort.Sort(result)
	}

	currentColumn = 0
	columns = make([]string, 0)

	return result
}
