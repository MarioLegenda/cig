package conditionResolver

type columnMetadata struct {
	columnsToReturn []int
	columnNames     []string
}

type ColumnMetadata interface {
	ColumnsToReturn() []int
	ColumnNames() []string
	Position(name string) int
}

func NewColumnMetadata(columnsToReturn []int, columnNames []string) ColumnMetadata {
	return columnMetadata{
		columnsToReturn: columnsToReturn,
		columnNames:     columnNames,
	}
}

func (cm columnMetadata) ColumnsToReturn() []int {
	return cm.columnsToReturn
}

func (cm columnMetadata) ColumnNames() []string {
	return cm.columnNames
}

func (cm columnMetadata) Position(name string) int {
	names := cm.columnNames
	for i, n := range names {
		if n == name {
			return i
		}
	}

	return -1
}
