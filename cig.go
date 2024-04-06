package cig

import (
	"github.com/MarioLegenda/cig/pkg/db"
	"github.com/MarioLegenda/cig/pkg/result"
	"github.com/MarioLegenda/cig/pkg/syntax"
)

type Cig interface {
	Run(sql string) result.Result[map[string]string]
	Close() result.Result[any]
}

type cig struct {
	db db.DB
}

func (c cig) Run(sql string) result.Result[map[string]string] {
	res := syntax.NewStructure(sql)
	if res.HasErrors() {
		return result.NewResult[map[string]string](nil, res.Errors())
	}

	c.db.Run(res.Result())

	return result.NewResult[map[string]string](nil, nil)
}

func (c cig) Close() result.Result[any] {
	return c.db.Close()
}

func New() Cig {
	return cig{db: db.New()}
}
