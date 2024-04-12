package syntaxParts

type column struct {
	columns []string
}

type Column interface {
	HasColumn(column string) int
	Columns() []string
}

func (c column) HasColumn(search string) int {
	for i, cl := range c.columns {
		if cl == search {
			return i
		}
	}

	return -1
}

func (c column) Columns() []string {
	return c.columns
}

func NewColumn(columns []string) Column {
	return column{columns: columns}
}
