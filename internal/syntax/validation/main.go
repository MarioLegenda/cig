package validation

type Limit = int64
type Offset = int64

type OrderByColumn struct {
	Alias  string
	Column string
}

type OrderBy struct {
	Columns   []OrderByColumn
	Direction string
}

type Condition struct {
	Alias              string
	Value              string
	Column             string
	DataType           string
	ComparisonOperator string
	LogicalOperator    string
}

type SelectableColumn struct {
	Alias    string
	Column   string
	Original string
}

type Metadata struct {
	SelectedColumns []SelectableColumn
	FilePath        string
	Alias           string
	Conditions      []Condition
	OrderBy         *OrderBy
	Limit           Limit
	Offset          Offset
}

func ValidateAndCreateMetadata(tokens []string) (Metadata, error) {
	// reserve enough space so not to get "index out of range"
	tokens = append(tokens, make([]string, 100)...)
	currentIdx := 0

	if err := validSelect(tokens); err != nil {
		return Metadata{}, err
	}
	currentIdx++

	skipIndex, selectableColumns, err := validSelectableColumns(tokens)
	if err != nil {
		return Metadata{}, err
	}

	currentIdx += skipIndex
	if err := validateFrom(tokens[currentIdx]); err != nil {
		return Metadata{}, err
	}
	currentIdx++

	path, err := validatePath(tokens[currentIdx])
	if err != nil {
		return Metadata{}, err
	}

	currentIdx++

	if err := validateAsToken(tokens[currentIdx]); err != nil {
		return Metadata{}, err
	}
	currentIdx++

	alias, err := validateAlias(tokens[currentIdx])
	if err != nil {
		return Metadata{}, err
	}

	if err := validateSelectableColumnAlias(tokens[currentIdx], selectableColumns); err != nil {
		return Metadata{}, err
	}
	currentIdx++

	if err := validateWhereClause(tokens[currentIdx]); err != nil {
		return Metadata{}, err
	}
	currentIdx++

	conditions, err := validateConditions(alias, tokens, currentIdx)
	if err != nil {
		return Metadata{}, err
	}
	currentIdx += len(conditions) * 3

	limit, offset, orderBy, err := validateConstraints(alias, tokens, currentIdx)

	return Metadata{
		SelectedColumns: selectableColumns,
		FilePath:        path,
		Alias:           alias,
		Conditions:      conditions,
		OrderBy:         orderBy,
		Offset:          offset,
		Limit:           limit,
	}, err
}
