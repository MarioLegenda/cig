package validation

import (
	"fmt"
	"github.com/MarioLegenda/cig/internal/syntax/dataTypes"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/pkg"
	"strconv"
	"strings"
)

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
