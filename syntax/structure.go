package syntax

import (
	"cig/syntax/corrector"
	"cig/syntax/splitter"
	"cig/syntax/syntaxParts"
	"fmt"
	"strings"
)

type structure struct {
	column syntaxParts.SyntaxType
	fileDb syntaxParts.SyntaxType
}

type Structure interface {
	Column() syntaxParts.SyntaxType
	FileDB() syntaxParts.SyntaxType
}

func (s structure) Column() syntaxParts.SyntaxType {
	return s.column
}

func (s structure) FileDB() syntaxParts.SyntaxType {
	return s.fileDb
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
		column: syntaxParts.NewColumn(columns),
		fileDb: syntaxParts.NewFileDB(f, alias),
	}

	resolveWhereClause(s.Chunks()[6:])

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

func resolveWhereClause(chunks []string) {
	fmt.Println(chunks)
}
