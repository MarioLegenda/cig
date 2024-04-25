package db

type metadataColumns []metadataColumn

type metadataColumn struct {
	position int
	name     string
}

func (mcs metadataColumns) getPositionByName(name string) int {
	for _, m := range mcs {
		if m.name == name {
			return m.position
		}
	}

	return -1
}

func (mcs metadataColumns) names() []string {
	names := make([]string, len(mcs))
	for i, m := range mcs {
		names[i] = m.name
	}

	return names
}
