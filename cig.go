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
	if res.Error() != nil {
		return nil, res.Error()
	}

	fsDb := db.New()
	dbResult := fsDb.Run(res.Result())

	if dbResult.Error != nil {
		return nil, dbResult.Error
	}

	return dbResult.Data, nil
}

func New() Cig {
	return cig{}
}
