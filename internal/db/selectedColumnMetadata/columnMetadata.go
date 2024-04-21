package selectedColumnMetadata

type columnMetadata struct {
	positions []int
	names     []string
}

type ColumnMetadata interface {
	Column(pos int) string
	HasPosition(pos int) bool
}

func (cm columnMetadata) Column(pos int) string {
	for p, s := range cm.positions {
		if pos == s {
			return cm.names[p]
		}
	}

	return ""
}

func (cm columnMetadata) HasPosition(pos int) bool {
	for _, s := range cm.positions {
		if s == pos {
			return true
		}
	}

	return false
}

func New(selectedColumns []string, allColumns []string) ColumnMetadata {
	positions := make([]int, 0)
	names := make([]string, 0)
	allSelected := false
	if len(selectedColumns) == 1 && selectedColumns[0] == "*" {
		allSelected = true
	}

	for i, s := range allColumns {
		for _, t := range selectedColumns {
			if allSelected {
				positions = append(positions, i)
				names = append(names, s)

				continue
			}

			if t == s {
				positions = append(positions, i)
				names = append(names, s)
			}
		}
	}

	return columnMetadata{
		positions: positions,
		names:     names,
	}
}
