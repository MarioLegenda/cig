package job

type MapResult map[string]string

type sortableResult struct {
	columns       []string
	result        SearchResult
	currentColumn string
}

func (s *sortableResult) Len() int {
	return len(s.columns)
}

func (s *sortableResult) Swap(i, j int) {
	s.result[i], s.result[j] = s.result[j], s.result[i]
}

func (s *sortableResult) Less(i, j int) bool {
	return s.result[i][s.currentColumn] < s.result[j][s.currentColumn]
}

type repeatableSort interface {
	changeColumn(c string)
}

func (s *sortableResult) changeColumn(c string) {
	s.currentColumn = c
}

func newSortableResult(result SearchResult, columns []string) repeatableSort {
	return &sortableResult{
		columns:       columns,
		result:        result,
		currentColumn: columns[0],
	}
}
