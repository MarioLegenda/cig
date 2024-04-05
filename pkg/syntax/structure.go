package syntax

import (
	"cig/pkg/syntax/corrector"
	"cig/pkg/syntax/splitter"
	syntaxParts2 "cig/pkg/syntax/syntaxParts"
	"strings"
)

type structure struct {
	column    syntaxParts2.SyntaxType
	fileDb    syntaxParts2.SyntaxType
	condition syntaxParts2.Condition
}

type Structure interface {
	Column() syntaxParts2.SyntaxType
	FileDB() syntaxParts2.SyntaxType
	Condition() syntaxParts2.Condition
}

func (s structure) Column() syntaxParts2.SyntaxType {
	return s.column
}

func (s structure) FileDB() syntaxParts2.SyntaxType {
	return s.fileDb
}

func (s structure) Condition() syntaxParts2.Condition {
	return s.condition
}

func NewStructure(sql string) Result[Structure] {
	s := splitter.NewSplitter(sql)
	errs := corrector.IsShallowSyntaxCorrect(s)
	if len(errs) != 0 {
		return NewResult[Structure](nil, errs)
	}

	columns := splitColumns(s.Chunks()[1])
	f, alias := resolveFiles(s.Chunks()[3], s.Chunks()[5])

	syntaxStructure := structure{
		column:    syntaxParts2.NewColumn(columns),
		fileDb:    syntaxParts2.NewFileDB(f, alias),
		condition: resolveWhereClause(s.Chunks()[6:]),
	}

	return NewResult[Structure](syntaxStructure, []error{})
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

func resolveWhereClause(chunks []string) syntaxParts2.Condition {
	return syntaxParts2.NewCondition(chunks[1], chunks[2], chunks[3])
}
