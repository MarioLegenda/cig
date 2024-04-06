package syntax

import (
	"github.com/MarioLegenda/cig/pkg/result"
	"github.com/MarioLegenda/cig/pkg/syntax/corrector"
	"github.com/MarioLegenda/cig/pkg/syntax/splitter"
	"github.com/MarioLegenda/cig/pkg/syntax/syntaxParts"
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
	return syntaxParts.NewCondition(chunks[1], chunks[2], chunks[3])
}
