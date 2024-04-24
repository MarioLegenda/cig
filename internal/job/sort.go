package job

import (
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
	"sort"
)

type MapResult map[string]string

type sortableResult struct {
	columns       []string
	result        SearchResult
	currentColumn int
}

func (s *sortableResult) Len() int {
	return len(s.columns)
}

func (s *sortableResult) Swap(i, j int) {
	s.result[i], s.result[j] = s.result[j], s.result[i]
}

func (s *sortableResult) Less(i, j int) bool {
	return s.result[i][s.columns[s.currentColumn]] < s.result[j][s.columns[s.currentColumn]]
}

func sortResults(result SearchResult, orderBy syntaxStructure.OrderBy) SearchResult {
	ssColumns := orderBy.Columns()
	columns := make([]string, len(ssColumns))
	for _, c := range ssColumns {
		columns = append(columns, c.Column())
	}

	sr := &sortableResult{
		columns: columns,
		result:  result,
	}

	for i, _ := range columns {
		sr.currentColumn = i
		sort.Sort(sr)
	}

	return sr.result
}
