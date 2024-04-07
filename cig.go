package cig

import (
	"github.com/MarioLegenda/cig/pkg/db"
	"github.com/MarioLegenda/cig/pkg/job"
	"github.com/MarioLegenda/cig/pkg/result"
	"github.com/MarioLegenda/cig/pkg/syntax"
)

type Cig interface {
	Run(sql string) result.Result[job.SearchResult]
	Close() result.Result[any]
}

type cig struct {
	db db.DB
}

func (c cig) Run(sql string) result.Result[job.SearchResult] {
	res := syntax.NewStructure(sql)
	if res.HasErrors() {
		return result.NewResult[job.SearchResult](nil, res.Errors())
	}

	dbResult := c.db.Run(res.Result())

	return result.NewResult[job.SearchResult](dbResult.Result(), dbResult.Errors())
}

func (c cig) Close() result.Result[any] {
	return c.db.Close()
}

func New() Cig {
	return cig{db: db.New()}
}
