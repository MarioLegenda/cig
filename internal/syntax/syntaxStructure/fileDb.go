package syntaxStructure

type fileDb struct {
	path  string
	alias string
}

type FileDB interface {
	Path() string
	Alias() string
}

func (f fileDb) Path() string {
	return f.path
}

func (f fileDb) Alias() string {
	return f.alias
}

func NewFileDB(path, alias string) FileDB {
	return fileDb{path: path, alias: alias}
}
