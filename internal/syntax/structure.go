package syntax

import (
	"github.com/MarioLegenda/cig/internal/syntax/corrector"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/splitter"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxParts"
	"github.com/MarioLegenda/cig/pkg/result"
	"strings"
)

type structure struct {
	column    syntaxParts.Column
	fileDb    syntaxParts.FileDB
	condition syntaxParts.Condition
}

type Structure interface {
	Column() syntaxParts.Column
	FileDB() syntaxParts.FileDB
	Condition() syntaxParts.Condition
}

func (s structure) Column() syntaxParts.Column {
	return s.column
}

func (s structure) FileDB() syntaxParts.FileDB {
	return s.fileDb
}

func (s structure) Condition() syntaxParts.Condition {
	return s.condition
}

func NewStructure(sql string) result.Result[Structure] {
	s := splitter.NewSplitter(sql)
	errs := corrector.IsShallowSyntaxCorrect(s)
	if len(errs) != 0 {
		return result.NewResult[Structure](nil, errs)
	}

	columns := splitColumns(s.Chunks()[1])
	f, alias := resolveFiles(s.Chunks()[3], s.Chunks()[5])

	syntaxStructure := structure{
		column:    syntaxParts.NewColumn(columns),
		fileDb:    syntaxParts.NewFileDB(f, alias),
		condition: resolveWhereClause(s.Chunks()[6:]),
	}

	return result.NewResult[Structure](syntaxStructure, nil)
}

func splitColumns(c string) []string {
	if c == "*" {
		return []string{"*"}
	}

	return strings.Split(c, ",")
}

func resolveFiles(path, alias string) (string, string) {
	p := strings.Split(path, ":")

	return p[1], alias
}

func resolveWhereClause(chunks []string) syntaxParts.Condition {
	if len(chunks) == 0 {
		return nil
	}

	conditionsOnly := chunks[1:]

	var head syntaxParts.Condition
	var next syntaxParts.Condition
	isDiscoveryMode := true
	position := 0
	var parts [3]string
	for _, c := range conditionsOnly {
		if !isDiscoveryMode {
			// TODO: check that next != nil to prevent runtime panic
			logicalOperator := operators.AndOperator
			if strings.ToLower(c) == operators.OrOperator {
				logicalOperator = operators.OrOperator
			}

			t := syntaxParts.NewCondition(
				nil,
				syntaxParts.NewConditionOperator(logicalOperator, c),
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

				head = syntaxParts.NewCondition(
					syntaxParts.NewConditionColumn(aliasSplit[0], aliasSplit[1], dataType, parts[0]),
					syntaxParts.NewConditionOperator(parts[1], parts[1]),
					syntaxParts.NewConditionValue(parts[2][1:len(parts[2])-1], parts[2]),
				)
			} else if head != nil && next != nil {
				columnOnly, dataType := getColumnDataOnlyIfDataTypeExists(parts[0])
				aliasSplit := strings.Split(columnOnly[1:len(columnOnly)-1], ".")

				t := syntaxParts.NewCondition(
					syntaxParts.NewConditionColumn(aliasSplit[0], aliasSplit[1], dataType, parts[0]),
					syntaxParts.NewConditionOperator(parts[1], parts[1]),
					syntaxParts.NewConditionValue(parts[2][1:len(parts[2])-1], parts[2]),
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

func getColumnDataOnlyIfDataTypeExists(column string) (string, string) {
	split := strings.Split(column, "::")
	if len(split) == 2 {
		return split[0], split[1]
	}

	return column, ""
}
