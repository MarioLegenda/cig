package syntaxParts

const ColumnType = "column"
const FileDBType = "fileDb"

type SyntaxType interface {
	Type() string
}
