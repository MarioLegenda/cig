package cig

import (
	"github.com/MarioLegenda/cig/internal/db"
	"github.com/MarioLegenda/cig/internal/job"
	"github.com/MarioLegenda/cig/internal/syntax"
	"github.com/MarioLegenda/cig/pkg/result"
)

type Cig interface {
	Run(sql string) result.Result[job.SearchResult]
}

type cig struct {
}

func (c cig) Run(sql string) result.Result[job.SearchResult] {
	res := syntax.NewStructure(sql)
	if res.HasErrors() {
		return result.NewResult[job.SearchResult](nil, res.Errors())
	}

	fsDb := db.New()
	dbResult := fsDb.Run(res.Result())

	return result.NewResult[job.SearchResult](dbResult.Result(), dbResult.Errors())
}

func New() Cig {
	return cig{}
}
