package cig

import (
	"github.com/MarioLegenda/cig/internal/db"
	"github.com/MarioLegenda/cig/internal/syntax"
)

type Cig interface {
	Run(sql string) ([]map[string]string, error)
}

type cig struct{}

func (c cig) Run(sql string) ([]map[string]string, error) {
	res := syntax.NewStructure(sql)
	if res.Errors() != nil {
		return nil, res.Errors()[0]
	}

	fsDb := db.New()
	dbResult := fsDb.Run(res.Result())

	if dbResult.Errors() != nil {
		return nil, dbResult.Errors()[0]
	}

	return dbResult.Result(), nil
}

func New() Cig {
	return cig{}
}
