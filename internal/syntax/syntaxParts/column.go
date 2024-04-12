package syntaxParts

type column struct {
	columns []string
}

type Column interface {
	HasColumn(column string) bool
	Columns() []string
}

func (c column) HasColumn(search string) bool {
	for _, cl := range c.columns {
		if cl == search {
			return true
		}
	}

	return false
}

func (c column) Columns() []string {
	return c.columns
}

func NewColumn(columns []string) Column {
	return column{columns: columns}
}
