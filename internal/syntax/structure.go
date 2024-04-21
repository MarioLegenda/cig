package syntax

import (
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
	"github.com/MarioLegenda/cig/internal/syntax/tokenizer"
	"github.com/MarioLegenda/cig/internal/syntax/validation"
	"github.com/MarioLegenda/cig/pkg/result"
	"strconv"
	"strings"
)

type structure struct {
	column      syntaxStructure.Column
	fileDb      syntaxStructure.FileDB
	condition   syntaxStructure.Condition
	constraints syntaxStructure.StructureConstraints
}

type Structure interface {
	Column() syntaxStructure.Column
	FileDB() syntaxStructure.FileDB
	Condition() syntaxStructure.Condition
	Constraints() syntaxStructure.StructureConstraints
}

func (s structure) Column() syntaxStructure.Column {
	return s.column
}

func (s structure) FileDB() syntaxStructure.FileDB {
	return s.fileDb
}

func (s structure) Condition() syntaxStructure.Condition {
	return s.condition
}

func (s structure) Constraints() syntaxStructure.StructureConstraints {
	return s.constraints
}

func NewStructure(sql string) result.Result[Structure] {
	tokens := tokenizer.Tokenize(sql)
	metadata, err := validation.ValidateAndCreateMetadata(tokens)
	if err != nil {
		return result.NewResult[Structure](nil, []error{err})
	}

	columns := make([]string, len(metadata.SelectedColumns))
	for i, c := range metadata.SelectedColumns {
		columns[i] = c.Column
	}

	t := structure{
		column:      syntaxStructure.NewColumn(columns),
		fileDb:      syntaxStructure.NewFileDB(metadata.FilePath, metadata.Alias),
		condition:   resolveWhereClause(metadata.Conditions),
		constraints: resolveConstraints([]string{}),
	}
	
	return result.NewResult[Structure](t, nil)
}

func combineNonSeparatableParts(chunks []string) []string {
	combinedChunks := make([]string, 0)

	combineMode := false
	base := ""
	for _, c := range chunks {
		if strings.ToLower(c) == "select" {
			combineMode = true
			combinedChunks = append(combinedChunks, c)
			continue
		}

		if strings.ToLower(c) == "from" {
			combinedChunks = append(combinedChunks, base)
			combineMode = false
		}

		if combineMode {
			base += c
		}

		if !combineMode {
			combinedChunks = append(combinedChunks, c)
		}
	}

	return combinedChunks
}

func splitColumns(c string) []string {
	if c == "*" {
		return []string{"*"}
	}

	split := strings.Split(c, ",")
	for i, s := range split {
		split[i] = s[1 : len(s)-1]
	}

	return split
}

func resolveFiles(path, alias string) (string, string) {
	p := strings.Split(path, ":")

	return p[1], alias
}

func resolveWhereClause(conditions []validation.Condition) syntaxStructure.Condition {
	if len(conditions) == 0 {
		return nil
	}

	var head syntaxStructure.Condition
	var next syntaxStructure.Condition
	for _, condition := range conditions {
		var logicalOperator string
		if condition.LogicalOperator == operators.AndOperator || condition.LogicalOperator == operators.OrOperator {
			logicalOperator = operators.AndOperator
			if strings.ToLower(condition.LogicalOperator) == operators.OrOperator {
				logicalOperator = operators.OrOperator
			}
		}

		if head == nil {
			head = syntaxStructure.NewCondition(
				syntaxStructure.NewConditionColumn(condition.Alias, condition.Column, condition.DataType, ""),
				syntaxStructure.NewConditionOperator(condition.ComparisonOperator, ""),
				syntaxStructure.NewConditionValue(condition.Value, ""),
			)

			if logicalOperator != "" {
				head.SetNext(syntaxStructure.NewCondition(
					nil,
					syntaxStructure.NewConditionOperator(logicalOperator, ""),
					nil,
				))
			}

			next = head.Next()

			continue
		}

		if next != nil {
			t := syntaxStructure.NewCondition(
				syntaxStructure.NewConditionColumn(condition.Alias, condition.Column, condition.DataType, ""),
				syntaxStructure.NewConditionOperator(condition.ComparisonOperator, ""),
				syntaxStructure.NewConditionValue(condition.Value, ""),
			)

			if logicalOperator != "" {
				t.SetNext(syntaxStructure.NewCondition(
					nil,
					syntaxStructure.NewConditionOperator(logicalOperator, ""),
					nil,
				))
			}

			next.SetNext(t)
			next = next.Next().Next()
		}
	}

	return head
}

func resolveConstraints(chunks []string) syntaxStructure.StructureConstraints {
	var limit syntaxStructure.Constraint[int64]
	var offset syntaxStructure.Constraint[int64]
	limitMode := false
	offsetMode := false
	for _, c := range chunks {
		if strings.ToLower(c) == "limit" {
			limitMode = true
			continue
		}

		if strings.ToLower(c) == "offset" {
			offsetMode = true
			continue
		}

		if limitMode {
			l, _ := strconv.ParseInt(c, 10, 64)
			limit = syntaxStructure.NewLimit(l)
			limitMode = false
		}

		if offsetMode {
			l, _ := strconv.ParseInt(c, 10, 64)
			offset = syntaxStructure.NewOffset(l)
			offsetMode = false
		}
	}

	return syntaxStructure.NewConstraints(limit, offset)
}

func getColumnDataOnlyIfDataTypeExists(column string) (string, string) {
	split := strings.Split(column, "::")
	if len(split) == 2 {
		return split[0], split[1]
	}

	return column, ""
}
