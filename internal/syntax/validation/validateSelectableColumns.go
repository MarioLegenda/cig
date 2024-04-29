package validation

import (
	"fmt"
	"github.com/MarioLegenda/cig/pkg"
	"sort"
	"strings"
)

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
