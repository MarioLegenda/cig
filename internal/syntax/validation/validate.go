package validation

import (
	"fmt"
	"github.com/MarioLegenda/cig/internal/syntax/dataTypes"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/pkg"
	"os"
	"sort"
	"strconv"
	"strings"
)

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
	currentIdx += len(conditions)*3 + 1

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

func validSelect(tokens []string) error {
	if strings.ToLower(tokens[0]) != "select" {
		return pkg.InvalidSelectToken
	}

	return nil
}

func validSelectableColumns(tokens []string) (int, []SelectableColumn, error) {
	if tokens[1] == "*" {
		return 1, []SelectableColumn{
			{
				Alias:    "",
				Column:   "*",
				Original: tokens[1],
			},
		}, nil
	}

	selectableColumns := make([]SelectableColumn, 0)
	columnNamesToValidate := make([]string, 0)

	nextToSkip := 0
	columnMode := true
	commaMode := false
	for i := 1; i < len(tokens); i++ {
		token := tokens[i]
		nextToSkip++

		if token == "" {
			return -1, nil, fmt.Errorf("Selectable column is invalid. Expected column, got something else: %w", pkg.InvalidSelectableColumns)
		}

		if columnMode {
			if !isEnclosedInQuote(token) {
				return -1, nil, fmt.Errorf("Selectable columns should be enclosed inside single quotes: %w", pkg.InvalidSelectableColumns)
			}

			// check proper column with alias
			columnOnly := token[1 : len(token)-1]
			splitted := strings.Split(columnOnly, ".")

			if len(splitted) != 2 {
				return -1, nil, fmt.Errorf("Selectable columns have to be in form {alias}.{columnName}: %w", pkg.InvalidSelectableColumns)
			}

			columnNamesToValidate = append(columnNamesToValidate, splitted[1])
			columnMode = false

			selectableColumns = append(selectableColumns, SelectableColumn{
				Alias:    splitted[0],
				Column:   splitted[1],
				Original: columnOnly,
			})

			nextPossibleColumn := i + 2
			// the next column is not a "column" but something else, stop validating selectable columns
			if tokens[i+1] == "," && isEnclosedInQuote(tokens[nextPossibleColumn]) {
				commaMode = true
				columnMode = false
				continue
			} else {
				break
			}
		}

		if commaMode {
			if token != "," {
				return -1, nil, fmt.Errorf("Invalid column separator. Expected comma (,), got something else: %w", pkg.InvalidSelectableColumns)
			}

			columnMode = true
			commaMode = false
		}
	}

	sort.Strings(columnNamesToValidate)
	for i, s := range columnNamesToValidate {
		if i < len(columnNamesToValidate)-1 {
			next := columnNamesToValidate[i+1]
			if next == s {
				return -1, nil, fmt.Errorf("Duplicate column found: %w", pkg.InvalidDuplicatedColumn)
			}
		}
	}

	return nextToSkip, selectableColumns, nil
}

func validateFrom(token string) error {
	if strings.ToLower(token) != "from" {
		return pkg.InvalidFromToken
	}

	return nil
}

func validatePath(token string) (string, error) {
	// validate csv file path
	splitPath := strings.Split(token, ":")
	if len(splitPath) != 2 {
		return "", pkg.InvalidFilePathToken
	}

	if splitPath[0] != "path" {
		return "", pkg.InvalidFilePathToken
	}

	// get the actual path part and validate that it exists
	path := splitPath[1]
	stat, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("File path %s does not exist: %w", path, pkg.InvalidFilePathToken)
	}

	// validate that the file is an actual .csv file
	nameSplit := strings.Split(stat.Name(), ".")
	if nameSplit[1] != "csv" {
		return "", fmt.Errorf("File %s is not a csv file or it does not have a csv extension: %w", path, pkg.InvalidFilePathToken)
	}

	return path, nil
}

func validateAsToken(token string) error {
	if strings.ToLower(token) != "as" {
		return pkg.InvalidAsToken
	}

	return nil
}

func validateAlias(token string) (string, error) {
	if token == "" {
		return "", pkg.InvalidAlias
	}

	return token, nil
}

func validateSelectableColumnAlias(alias string, selectableColumns []SelectableColumn) error {
	if len(selectableColumns) == 1 && selectableColumns[0].Column == "*" {
		return nil
	}

	for _, c := range selectableColumns {
		if c.Alias != alias {
			return fmt.Errorf("Expected alias %s, got %s for column %s: %w", alias, c.Alias, c.Column, pkg.InvalidColumnAlias)
		}
	}

	return nil
}

func validateWhereClause(token string) error {
	if token == "" {
		return nil
	}
	if strings.ToLower(token) != "where" {
		return pkg.InvalidWhereClause
	}

	return nil
}

func validateConditions(alias string, tokens []string, startIdx int) ([]Condition, error) {
	if tokens[startIdx] == "" {
		return []Condition{}, nil
	}

	column := tokens[startIdx]
	operator := tokens[startIdx+1]
	value := tokens[startIdx+2]

	conditions := make([]Condition, 0)
	if column == "limit" || column == "offset" || column == "order" {
		return conditions, nil
	}

	getColumnAndDataType := func(c string) (string, string) {
		dtSplit := strings.Split(c, "::")

		var columnOnly string
		var dataType string
		if len(dtSplit) == 2 {
			columnOnly = dtSplit[0]
			dataType = dtSplit[1]
		} else {
			columnOnly = c
		}

		return columnOnly, dataType
	}

	validateColumn := func(c string) (string, error) {
		if !isEnclosedInQuote(c) {
			return "", pkg.InvalidSelectableColumns
		}

		columnOnly := c[1 : len(c)-1]
		splitted := strings.Split(columnOnly, ".")

		if len(splitted) != 2 {
			return "", fmt.Errorf("Condition column have to be in form {alias}.{columnName}: %w", pkg.InvalidConditionColumn)
		}

		if splitted[0] != alias {
			return "", fmt.Errorf("Invalid condition column alias. Expected %s: %w", alias, pkg.InvalidConditionAlias)
		}

		return splitted[1], nil
	}

	validateDataType := func(dt string) error {
		for _, d := range dataTypes.DataTypes {
			if d == dt {
				return nil
			}
		}

		return fmt.Errorf("Invalid data type. Expected one of %s, got something else: %w", strings.Join(dataTypes.DataTypes, ","), pkg.InvalidDataType)
	}

	extractedColumn, dataType := getColumnAndDataType(column)
	columnOnly, err := validateColumn(extractedColumn)

	if err != nil {
		return conditions, err
	}

	found := false
	for _, o := range operators.Operators {
		if operator == o {
			found = true
			break
		}
	}

	if !found {
		return conditions, pkg.InvalidComparisonOperator
	}

	if !isEnclosedInQuote(value) {
		return conditions, pkg.InvalidValueToken
	}

	if dataType != "" {
		if err := validateDataType(dataType); err != nil {
			return conditions, err
		}

		if dataType == dataTypes.Int {
			_, err := strconv.ParseInt(value[1:len(value)-1], 10, 64)
			if err != nil {
				return conditions, fmt.Errorf("Expected a valid integer, got something else: %w", pkg.InvalidDataType)
			}
		}

		if dataType == dataTypes.Float {
			_, err := strconv.ParseFloat(value[1:len(value)-1], 64)
			if err != nil {
				return conditions, fmt.Errorf("Expected a valid float, got something else: %w", pkg.InvalidDataType)
			}
		}
	}

	logicalOperator := strings.ToLower(tokens[startIdx+3])

	condition := Condition{
		Alias:              alias,
		Column:             columnOnly,
		Value:              value[1 : len(value)-1],
		DataType:           dataType,
		ComparisonOperator: operator,
	}

	if logicalOperator != operators.AndOperator && logicalOperator != operators.OrOperator {
		conditions = append(conditions, condition)

		return conditions, nil
	}

	if logicalOperator == operators.AndOperator || logicalOperator == operators.OrOperator {
		condition.LogicalOperator = logicalOperator
		conditions = append(conditions, condition)

		c, err := validateConditions(alias, tokens, startIdx+4)
		if err != nil {
			return conditions, err
		}

		conditions = append(conditions, c...)
	} else {
		conditions = append(conditions, condition)
	}

	return conditions, nil
}

func validateConstraints(alias string, tokens []string, startIdx int) (Limit, Offset, *OrderBy, error) {
	orderByColumns := make([]OrderByColumn, 0)
	var direction string
	var offset Offset = -1
	var limit Limit = -1

	validateColumn := func(alias, c string) (string, error) {
		if !isEnclosedInQuote(c) {
			return "", fmt.Errorf("Invalid ORDER BY column. Colums must be enclosed by single quotes: %w", pkg.InvalidOrderBy)
		}

		columnOnly := c[1 : len(c)-1]
		splitted := strings.Split(columnOnly, ".")
		if len(splitted) != 2 {
			return "", fmt.Errorf("Invalid ORDER BY column. Column does not specify an alias: %w", pkg.InvalidOrderBy)
		}

		if splitted[0] != alias {
			return "", fmt.Errorf("Invalid ORDER BY column. Expected alias %s, got %s: %w", alias, splitted[0], pkg.InvalidOrderBy)
		}

		return columnOnly, nil
	}

	/**
		Order by validation

		1. ORDER BY must follow at least one column
	    	1.1. If the next token is a ",", then the token after that MUST be a column
			1.2. If the next token is not a ",", the token after that CAN be either DESC or ASC
			1.3. If the token is not DESC or ASC, consider ORDER BY validated and move on
	*/
	for i := startIdx; i < len(tokens); i++ {
		token := strings.ToLower(tokens[i])

		// end of line, only appended buffers after this
		if token == "" {
			return limit, offset, &OrderBy{
				Columns:   orderByColumns,
				Direction: direction,
			}, nil
		}

		if token == "order" {
			// token after order must be "by"
			if strings.ToLower(tokens[i+1]) != "by" {
				return limit, offset, nil, fmt.Errorf("Expected BY, got something else: %w", pkg.InvalidOrderBy)
			}

			// this must be a column
			firstColumn := tokens[i+2]
			resolvedColumn, err := validateColumn(alias, firstColumn)
			if err != nil {
				return limit, offset, nil, err
			}

			orderByColumns = append(orderByColumns, OrderByColumn{
				Alias:  alias,
				Column: resolvedColumn,
			})

			// advance the pointer to be after "by" and the first column
			a := i + 3
			// this loop must not go to the end of all tokens
			for a < len(tokens) {
				comma := tokens[a]

				if comma == "," {
					nextColumn := a + 1
					resolvedColumn, err := validateColumn(alias, tokens[nextColumn])
					if err != nil {
						return limit, offset, nil, err
					}

					orderByColumns = append(orderByColumns, OrderByColumn{
						Alias:  alias,
						Column: resolvedColumn,
					})

					a = a + 2
					continue
				} else if strings.ToLower(comma) == "desc" || strings.ToLower(comma) == "asc" {
					direction = operators.Desc
					if strings.ToLower(comma) == "asc" {
						direction = operators.Asc
					}
				}

				break
			}
		} else if token == "offset" {
			nextToken := tokens[i+1]

			value, err := strconv.ParseInt(nextToken, 10, 64)
			if err != nil {
				return 0, 0, nil, fmt.Errorf("Expected OFFSET to be a valid integer, got something else: %w: %w", err, pkg.InvalidOrderBy)
			}

			offset = value
		} else if token == "offset" {
			nextToken := tokens[i+1]

			value, err := strconv.ParseInt(nextToken, 10, 64)
			if err != nil {
				return 0, 0, nil, fmt.Errorf("Expected OFFSET to be a valid integer, got something else: %w: %w", err, pkg.InvalidOrderBy)
			}

			offset = value
		} else if token == "limit" {
			nextToken := tokens[i+1]

			value, err := strconv.ParseInt(nextToken, 10, 64)
			if err != nil {
				return 0, 0, nil, fmt.Errorf("Expected LIMIT to be a valid integer, got something else: %w: %w", err, pkg.InvalidOrderBy)
			}

			limit = value
		}
	}

	return limit, offset, &OrderBy{
		Columns:   orderByColumns,
		Direction: direction,
	}, nil
}
