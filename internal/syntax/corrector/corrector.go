package corrector

import (
	"errors"
	"fmt"
	"github.com/MarioLegenda/cig/internal/syntax/dataTypes"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/splitter"
	"os"
	"sort"
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
var InvalidDataType = errors.New("Invalid data type.")
var InvalidAlias = errors.New("Invalid alias.")
var InvalidConditionAlias = errors.New("Invalid condition alias.")
var InvalidSelectedColumn = errors.New("Invalid selected column.")

func IsShallowSyntaxCorrect(s splitter.Splitter) []error {
	errs := make([]error, 0)
	chunks := normalizeChunks(s.Chunks())

	// there should be minimally 6 chunks, invalid right away
	if len(chunks) < 6 {
		errs = append(errs, InvalidNumberOfChunks)
		return errs
	}

	// if the first chunk is not select, invalid
	if strings.ToLower(chunks[0]) != "select" {
		errs = append(errs, InvalidSelectChunk)
	}

	// skip the columns validation for now, and validate that FROM is in the right position
	if strings.ToLower(chunks[2]) != "from" {
		errs = append(errs, InvalidFromChunk)
	}

	// validate csv file path
	splitPath := strings.Split(chunks[3], ":")
	if len(splitPath) != 2 {
		errs = append(errs, InvalidFilePathChunk)
		return errs
	}

	if splitPath[0] != "path" {
		errs = append(errs, InvalidFilePathChunk)
		return errs
	}

	// get the actual path part and validate that it exists
	path := splitPath[1]
	stat, err := os.Stat(path)
	if err != nil {
		errs = append(errs, fmt.Errorf("File path %s does not exist: %w", path, InvalidFilePath))
		return errs
	}

	// validate that the file is an actual .csv file
	nameSplit := strings.Split(stat.Name(), ".")
	if nameSplit[1] != "csv" {
		errs = append(errs, fmt.Errorf("File %s is not a csv file or it does not have a csv extension: %w", path, InvalidFilePath))
	}

	// validate that AS is in the right position
	if strings.ToLower(chunks[4]) != "as" {
		errs = append(errs, InvalidAsChunk)
	}

	// check that selected columns have the right selected alias in AS clause, and validate that there are no duplicated selected columns
	if aliasErrs := checkAliasAndSelectedColumnDuplicates(chunks[5], chunks[1]); aliasErrs != nil {
		errs = append(errs, aliasErrs...)
	}

	alias := chunks[5]
	// from index 6, there should be a where clause
	// TODO: must be changed when JOIN comes into play
	whereClause := chunks[6:]

	if len(whereClause) != 0 {
		// minimal number of chunks for WHERE clause is 4
		if len(whereClause) < 4 {
			errs = append(errs, fmt.Errorf("Expected at least a single condition for WHERE clause but got something else: %w", InvalidWhereClause))
			return errs
		}

		// validate actual WHERE clause
		where := whereClause[0]
		if strings.ToLower(where) != "where" {
			errs = append(errs, fmt.Errorf("Expected WHERE, got %s: %w", whereClause[0], InvalidWhereClause))
		}

		// after WHERE only conditions can be
		conditionParts := whereClause[1:]

		/**
			The algorithm iterates over each chunks of the condition. Condition is everything between
		    AND and OR operators. If the algorithm expects a condition, isDiscoveryMode will be true.
			If it expects an operator, it is false. Based on this flag, the algorithm will either validate
			the logical operator or the condition.

			Since conditionParts is an array simply split by an empty space, condition will be a combination
			of 3 array indexes. If the algorithm expects a condition, it will count those positions. That is the
			job of the position variable. This variable is incremented every time the algorithm expects a part of
			the condition and that part is saved into condition [3]string. Number of condition parts is to. After
			sufficient number of parts is found, the entire condition is validated.
		*/
		isDiscoveryMode := true
		var condition [3]string
		position := 0
		for _, k := range conditionParts {
			// logical operator expected and validated
			if !isDiscoveryMode {
				if err := checkLogicalOperator(k); err != nil {
					errs = append(errs, err)
				}

				isDiscoveryMode = true
				continue
			}

			// save the condition parts until there are 3 of them (position variable)
			if isDiscoveryMode {
				condition[position] = k
			}

			// all condition parts have been collected. validate the entire condition.
			if position == 2 {
				isDiscoveryMode = false
				position = 0

				if err := checkIsQuoteEnclosed(condition[0], "column"); err != nil {
					errs = append(errs, err)
				}

				if err := checkValidConditionAlias(alias, condition[0]); err != nil {
					errs = append(errs, err)
				}

				if err := checkDataTypeValidIfExists(condition[0]); err != nil {
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

			// this variable is only incremented if isDiscoveryMode = true which means
			// that the algorithm is collecting condition parts. It is reset to 0 (zero)
			// when the algorithm finds a logical operator
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
	split := strings.Split(v, "::")

	if len(split) == 2 {
		v = split[0]
	}

	if v[0] != '\'' || v[len(v)-1] != '\'' {
		return fmt.Errorf("Invalid %s value. Comparison values should be enclosed in single quotes: %w", t, InvalidValueChuck)
	}

	return nil
}

func checkAliasAndSelectedColumnDuplicates(alias, columns string) []error {
	if columns == "*" {
		return nil
	}

	split := strings.Split(columns, ",")
	sort.Strings(split)
	errs := make([]error, 0)

	prevClm := ""
	for _, s := range split {
		a := string(s[1])

		if alias != a {
			errs = append(errs, fmt.Errorf("Alias for column %s does not match the csv file alias %s: %w", a, alias, InvalidAlias))
		}

		if prevClm != "" && prevClm == s {
			errs = append(errs, fmt.Errorf("Duplicate column %s and %s found: %w", prevClm, s, InvalidSelectedColumn))
		}

		prevClm = s
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

func checkDataTypeValidIfExists(v string) error {
	split := strings.Split(v, "::")

	if len(split) == 1 {
		return nil
	}

	dt := split[1]
	for _, t := range dataTypes.DataTypes {
		if t == dt {
			return nil
		}
	}

	return fmt.Errorf("Invalid data type. Type %s does not exist. Valid conversion data types are %s: %w", dt, strings.Join(dataTypes.DataTypes, ","), InvalidDataType)
}

func checkValidConditionAlias(alias, column string) error {
	if alias != string(column[1]) {
		return fmt.Errorf("Condition alias does not correspond to csv file alias: alias: %s, column: %s: %w", alias, column, InvalidConditionAlias)
	}

	return nil
}

func normalizeChunks(chunks []string) []string {
	c := make([]string, 0)
	// append select statement
	c = append(c, chunks[0])

	withoutSelect := chunks[1:]
	appendOnlyMode := false
	columns := ""
	for _, k := range withoutSelect {
		if strings.ToLower(k) == "from" {
			c = append(c, columns)
			appendOnlyMode = true
		}

		if appendOnlyMode {
			c = append(c, k)
		}

		if !appendOnlyMode {
			columns += k
		}
	}

	return c
}
