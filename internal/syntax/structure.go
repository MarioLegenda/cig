package syntax

import (
	"fmt"
	"github.com/MarioLegenda/cig/internal/syntax/corrector"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/splitter"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
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
	s := splitter.NewSplitter(sql)
	errs := corrector.IsShallowSyntaxCorrect(s)
	if len(errs) != 0 {
		return result.NewResult[Structure](nil, errs)
	}

	columns := splitColumns(s.Chunks()[1])

	f, alias := resolveFiles(s.Chunks()[3], s.Chunks()[5])

	t := structure{
		column:      syntaxStructure.NewColumn(columns),
		fileDb:      syntaxStructure.NewFileDB(f, alias),
		condition:   resolveWhereClause(s.Chunks()[6:]),
		constraints: resolveConstraints(s.Chunks()[6:]),
	}

	return result.NewResult[Structure](t, nil)
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

func resolveWhereClause(chunks []string) syntaxStructure.Condition {
	if len(chunks) == 0 {
		return nil
	}

	conditionsOnly := chunks[1:]

	var head syntaxStructure.Condition
	var next syntaxStructure.Condition
	isDiscoveryMode := true
	position := 0
	var parts [3]string
	for _, c := range conditionsOnly {
		possibleConstraint := strings.ToLower(c)
		if possibleConstraint == operators.LimitConstraint || possibleConstraint == operators.OffsetConstraint || possibleConstraint == operators.OrderByConstraint {
			return head
		}

		if !isDiscoveryMode {
			logicalOperator := operators.AndOperator
			if strings.ToLower(c) == operators.OrOperator {
				logicalOperator = operators.OrOperator
			}

			t := syntaxStructure.NewCondition(
				nil,
				syntaxStructure.NewConditionOperator(logicalOperator, c),
				nil,
			)

			if head != nil && next == nil {
				head.SetNext(t)
				next = t
			} else if head != nil && next != nil {
				next.SetNext(t)
				next = t
			}

			isDiscoveryMode = true
			continue
		}

		if isDiscoveryMode {
			parts[position] = c
		}

		if position == 2 {
			isDiscoveryMode = false
			position = 0

			if head == nil {
				columnOnly, dataType := getColumnDataOnlyIfDataTypeExists(parts[0])
				aliasSplit := strings.Split(columnOnly[1:len(columnOnly)-1], ".")

				head = syntaxStructure.NewCondition(
					syntaxStructure.NewConditionColumn(aliasSplit[0], aliasSplit[1], dataType, parts[0]),
					syntaxStructure.NewConditionOperator(parts[1], parts[1]),
					syntaxStructure.NewConditionValue(parts[2][1:len(parts[2])-1], parts[2]),
				)
			} else if head != nil && next != nil {
				columnOnly, dataType := getColumnDataOnlyIfDataTypeExists(parts[0])
				aliasSplit := strings.Split(columnOnly[1:len(columnOnly)-1], ".")

				t := syntaxStructure.NewCondition(
					syntaxStructure.NewConditionColumn(aliasSplit[0], aliasSplit[1], dataType, parts[0]),
					syntaxStructure.NewConditionOperator(parts[1], parts[1]),
					syntaxStructure.NewConditionValue(parts[2][1:len(parts[2])-1], parts[2]),
				)

				next.SetNext(t)
				next = t
			}

			for i := 0; i < len(parts); i++ {
				parts[i] = ""
			}

			continue
		}

		position++
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
			fmt.Println(c)
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
