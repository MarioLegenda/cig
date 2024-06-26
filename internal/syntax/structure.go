package syntax

import (
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
	"github.com/MarioLegenda/cig/internal/syntax/tokenizer"
	"github.com/MarioLegenda/cig/internal/syntax/validation"
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

func NewStructure(sql string) (Structure, error) {
	tokens := tokenizer.Tokenize(sql)
	metadata, err := validation.ValidateAndCreateMetadata(tokens)
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(metadata.SelectedColumns))
	for i, c := range metadata.SelectedColumns {
		columns[i] = c.Column
	}

	t := structure{
		column:      syntaxStructure.NewColumn(columns),
		fileDb:      syntaxStructure.NewFileDB(metadata.FilePath, metadata.Alias),
		condition:   resolveWhereClause(metadata.Conditions),
		constraints: resolveConstraints(metadata.Limit, metadata.Offset, metadata.OrderBy),
	}

	return t, nil
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

func resolveConstraints(l int64, o int64, ob *validation.OrderBy) syntaxStructure.StructureConstraints {
	var limit syntaxStructure.Constraint[int64]
	var offset syntaxStructure.Constraint[int64]
	var orderBy syntaxStructure.OrderBy

	if l != -1 {
		limit = syntaxStructure.NewLimit(l)
	}

	if o != -1 {
		offset = syntaxStructure.NewOffset(o)
	}

	if ob != nil {
		mapping := make(map[string]string)
		for _, c := range ob.Columns {
			mapping[c.Column] = c.Alias
		}

		orderBy = syntaxStructure.NewOrderBy(mapping, ob.Direction)
	}

	return syntaxStructure.NewConstraints(limit, offset, orderBy)
}
