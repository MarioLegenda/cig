package validation

import (
	"errors"
	"fmt"
	"github.com/MarioLegenda/cig/internal/syntax/dataTypes"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"os"
	"sort"
	"strings"
)

var InvalidSelectToken = errors.New("Expected 'select', got something else.")
var InvalidSelectableColumns = errors.New("Expected selectable column")
var InvalidDuplicatedColumn = errors.New("Duplicated selectable column")
var InvalidFromToken = errors.New("Expected 'FROM', got something else.")
var InvalidFilePathToken = errors.New("Expected 'path:path_to_file' but did not get the path part")
var InvalidAsToken = errors.New("Expected 'as', got something else.")
var InvalidAlias = errors.New("Invalid alias.")
var InvalidColumnAlias = errors.New("Column alias not recognized.")
var InvalidWhereClause = errors.New("Expected WHERE clause, got something else.")
var InvalidConditionColumn = errors.New("Expected condition column.")
var InvalidComparisonOperator = errors.New("Invalid comparison operator")
var InvalidLogicalOperator = errors.New("Invalid logical operator")
var InvalidValueToken = errors.New("Invalid value token.")
var InvalidDataType = errors.New("Invalid data type.")
var InvalidConditionAlias = errors.New("Invalid condition alias.")

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

	return Metadata{
		SelectedColumns: selectableColumns,
		FilePath:        path,
		Alias:           alias,
		Conditions:      conditions,
	}, err
}

func validSelect(tokens []string) error {
	if strings.ToLower(tokens[0]) != "select" {
		return InvalidSelectToken
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
			return -1, nil, fmt.Errorf("Selectable column is invalid. Expected column, got something else: %w", InvalidSelectableColumns)
		}

		if columnMode {
			if !isEnclosedInQuote(token) {
				return -1, nil, fmt.Errorf("Selectable columns should be enclosed inside single quotes: %w", InvalidSelectableColumns)
			}

			// check proper column with alias
			columnOnly := token[1 : len(token)-1]
			splitted := strings.Split(columnOnly, ".")

			if len(splitted) != 2 {
				return -1, nil, fmt.Errorf("Selectable columns have to be in form {alias}.{columnName}: %w", InvalidSelectableColumns)
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
				return -1, nil, fmt.Errorf("Invalid column separator. Expected comma (,), got something else: %w", InvalidSelectableColumns)
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
				return -1, nil, fmt.Errorf("Duplicate column found: %w", InvalidDuplicatedColumn)
			}
		}
	}

	return nextToSkip, selectableColumns, nil
}

func validateFrom(token string) error {
	if strings.ToLower(token) != "from" {
		return InvalidFromToken
	}

	return nil
}

func validatePath(token string) (string, error) {
	// validate csv file path
	splitPath := strings.Split(token, ":")
	if len(splitPath) != 2 {
		return "", InvalidFilePathToken
	}

	if splitPath[0] != "path" {
		return "", InvalidFilePathToken
	}

	// get the actual path part and validate that it exists
	path := splitPath[1]
	stat, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("File path %s does not exist: %w", path, InvalidFilePathToken)
	}

	// validate that the file is an actual .csv file
	nameSplit := strings.Split(stat.Name(), ".")
	if nameSplit[1] != "csv" {
		return "", fmt.Errorf("File %s is not a csv file or it does not have a csv extension: %w", path, InvalidFilePathToken)
	}

	return path, nil
}

func validateAsToken(token string) error {
	if strings.ToLower(token) != "as" {
		return InvalidAsToken
	}

	return nil
}

func validateAlias(token string) (string, error) {
	if token == "" {
		return "", InvalidAlias
	}

	return token, nil
}

func validateSelectableColumnAlias(alias string, selectableColumns []SelectableColumn) error {
	if len(selectableColumns) == 1 && selectableColumns[0].Column == "*" {
		return nil
	}

	for _, c := range selectableColumns {
		if c.Alias != alias {
			return fmt.Errorf("Expected alias %s, got %s for column %s: %w", alias, c.Alias, c.Column, InvalidColumnAlias)
		}
	}

	return nil
}

func validateWhereClause(token string) error {
	if token == "" {
		return nil
	}
	if strings.ToLower(token) != "where" {
		return InvalidWhereClause
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
			return "", InvalidSelectableColumns
		}

		columnOnly := c[1 : len(c)-1]
		splitted := strings.Split(columnOnly, ".")

		if len(splitted) != 2 {
			return "", fmt.Errorf("Condition column have to be in form {alias}.{columnName}: %w", InvalidConditionColumn)
		}

		if splitted[0] != alias {
			return "", fmt.Errorf("Invalid condition column alias. Expected %s: %w", alias, InvalidConditionAlias)
		}

		return splitted[1], nil
	}

	validateDataType := func(dt string) error {
		for _, d := range dataTypes.DataTypes {
			if d == dt {
				return nil
			}
		}

		return fmt.Errorf("Invalid data type. Expected one of %s, got something else: %w", strings.Join(dataTypes.DataTypes, ","), InvalidDataType)
	}

	extractedColumn, dataType := getColumnAndDataType(column)
	columnOnly, err := validateColumn(extractedColumn)

	if err != nil {
		return conditions, err
	}

	if dataType != "" {
		if err := validateDataType(dataType); err != nil {
			return conditions, err
		}
	}

	found := false
	for _, o := range operators.Operators {
		if operator == o {
			found = true
			break
		}
	}

	if !found {
		return conditions, InvalidComparisonOperator
	}

	if !isEnclosedInQuote(value) {
		return conditions, InvalidValueToken
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
