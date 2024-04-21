package pkg

import "errors"

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
