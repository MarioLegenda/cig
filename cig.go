package cig

import (
	"github.com/MarioLegenda/cig/internal/db"
	"github.com/MarioLegenda/cig/internal/syntax"
)

type Cig interface {
	Run(sql string) Data
}

type cig struct{}

type Data struct {
	SelectedColumns []string
	AllColumns      []string
	Error           error
	Data            []map[string]string
}

func (c cig) Run(sql string) Data {
	res, err := syntax.NewStructure(sql)
	if err != nil {
		return newData(nil, nil, nil, err)
	}

	data := db.New().Run(res)

	return newData(data.SelectedColumns, data.AllColumns, data.Data, data.Error)
}

func New() Cig {
	return cig{}
}

func newData(selected, all []string, data []map[string]string, err error) Data {
	return Data{
		SelectedColumns: selected,
		AllColumns:      all,
		Error:           err,
		Data:            data,
	}
}
