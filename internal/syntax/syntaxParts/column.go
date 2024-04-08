package syntaxParts

type column struct {
	columns []string
}

type Column interface {
	HasColumn(column string) int
	ShouldReturnAll() bool
}

func (c column) HasColumn(search string) int {
	for i, cl := range c.columns {
		if cl == search {
			return i
		}
	}

	return -1
}

func (c column) ShouldReturnAll() bool {
	return len(c.columns) == 1 && c.columns[0] == "*"
}

func NewColumn(columns []string) Column {
	return column{columns: columns}
}
