package pkg

import (
	"cig/pkg/db"
	"cig/pkg/syntax"
)

type Cig interface {
	Run(sql string) (map[string]string, []error)
}

type cig struct {
}

func (c cig) Run(sql string) (map[string]string, []error) {
	res := syntax.NewStructure(sql)
	if res.HasErrors() {
		return nil, res.Errors()
	}

	dbRunner := db.New(res.Result())

	dbRunner.Run()

	return nil, nil
}

func New() Cig {
	return cig{}
}
