package validation

import (
	"fmt"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/pkg"
	"strconv"
	"strings"
)

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
