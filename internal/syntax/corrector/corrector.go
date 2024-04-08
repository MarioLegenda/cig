package corrector

import (
	"errors"
	"fmt"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/splitter"
	"os"
	"strings"
)

var InvalidNumberOfChunks = errors.New("Invalid number of chunks. Minimum number of syntax chunks is 6.")
var InvalidSelectChunk = errors.New("Expected 'select', got something else.")
var InvalidAsChunk = errors.New("Expected 'as', got something else.")
var InvalidFromChunk = errors.New("Expected 'from', got something else.")
var InvalidFilePathChunk = errors.New("Expected 'path:path_to_file' but did not get the path: part")
var InvalidFilePath = errors.New("Invalid file path.")
var InvalidWhereClause = errors.New("Invalid WHERE clause.")
var InvalidValueChuck = errors.New("Invalid value chunk.")

func IsShallowSyntaxCorrect(s splitter.Splitter) []error {
	chunks := s.Chunks()
	errs := make([]error, 0)

	if len(chunks) < 6 {
		errs = append(errs, InvalidNumberOfChunks)
		return errs
	}

	if strings.ToLower(chunks[0]) != "select" {
		errs = append(errs, InvalidSelectChunk)
	}

	if strings.ToLower(chunks[2]) != "from" {
		errs = append(errs, InvalidFromChunk)
	}

	splitPath := strings.Split(chunks[3], ":")
	if len(splitPath) != 2 {
		errs = append(errs, InvalidFilePathChunk)
		return errs
	}

	if splitPath[0] != "path" {
		errs = append(errs, InvalidFilePathChunk)
		return errs
	}

	path := splitPath[1]
	stat, err := os.Stat(path)
	if err != nil {
		errs = append(errs, fmt.Errorf("File path %s does not exist: %w", path, InvalidFilePath))
		return errs
	}

	nameSplit := strings.Split(stat.Name(), ".")
	if nameSplit[1] != "csv" {
		errs = append(errs, fmt.Errorf("File %s is not a csv file or it does not have a csv extension: %w", path, InvalidFilePath))
	}

	if strings.ToLower(chunks[4]) != "as" {
		errs = append(errs, InvalidAsChunk)
	}

	whereClause := chunks[6:]

	if len(whereClause) != 0 {
		if len(whereClause) < 4 {
			errs = append(errs, fmt.Errorf("Expected at least a single condition for WHERE clause but got something else: %w", InvalidWhereClause))
			return errs
		}

		where := whereClause[0]
		if strings.ToLower(where) != "where" {
			errs = append(errs, fmt.Errorf("Expected WHERE, got %s: %w", whereClause[0], InvalidWhereClause))
		}

		conditionParts := whereClause[1:]

		isDiscoveryMode := true
		var condition [3]string
		position := 0
		for _, k := range conditionParts {
			if !isDiscoveryMode {
				if err := checkLogicalOperator(k); err != nil {
					errs = append(errs, err)
				}

				isDiscoveryMode = true
				continue
			}

			if isDiscoveryMode {
				condition[position] = k
			}

			if position == 2 {
				isDiscoveryMode = false
				position = 0

				if err := checkIsQuoteEnclosed(condition[0], "column"); err != nil {
					errs = append(errs, err)
				}

				if err := checkConditionalOperator(condition[1]); err != nil {
					errs = append(errs, err)
				}

				if err := checkIsQuoteEnclosed(condition[2], "value"); err != nil {
					errs = append(errs, err)
				}

				for i := 0; i < len(condition); i++ {
					condition[0] = ""
				}

				continue
			}

			position++
		}
	}

	return errs
}

func checkConditionalOperator(op string) error {
	found := false
	for _, v := range operators.Operators {
		if v == op {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("Expected one of valid operators %s, got %s: %w", strings.Join(operators.Operators, ","), op, InvalidWhereClause)
	}

	return nil
}

func checkLogicalOperator(op string) error {
	t := strings.ToLower(op)
	if t != operators.AndOperator && t != operators.OrOperator {
		return fmt.Errorf("Expected AND or OR logical operators, got %s: %w", op, InvalidWhereClause)
	}

	return nil
}

func checkIsQuoteEnclosed(v, t string) error {
	if v[0] != '\'' || v[len(v)-1] != '\'' {
		return fmt.Errorf("Invalid %s value. Comparison values should be enclosed in single quotes: %w", t, InvalidValueChuck)
	}

	return nil
}
