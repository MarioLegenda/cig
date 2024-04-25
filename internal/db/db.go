package db

import (
	"context"
	"github.com/MarioLegenda/cig/internal/db/conditionResolver"
	"github.com/MarioLegenda/cig/internal/db/selectedColumnMetadata"
	job2 "github.com/MarioLegenda/cig/internal/job"
	"github.com/MarioLegenda/cig/internal/syntax"
	"time"
)

type fileMetadata struct {
	columns      metadataColumns
	originalPath string
}

type db struct {
	files map[string]fileMetadata
}

type DB interface {
	Run(s syntax.Structure) Data
}

type Data struct {
	SelectedColumns []string
	AllColumns      []string
	Error           error
	Data            []map[string]string
}

func (d *db) Run(s syntax.Structure) Data {
	file := s.FileDB()

	fileHandler, err := prepareRun(file, d)
	if err != nil {
		return newData(nil, nil, nil, err)
	}

	fsMetadata := d.files[file.Alias()]
	conditionColumnMetadata := createConditionColumnMetadata(fsMetadata)
	selectedColumns := createSelectedColumnMetadata(s, fsMetadata)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	res, err := job2.SearchFactory(selectedColumns, conditionColumnMetadata, s.Condition(), s.Constraints(), fileHandler)(0, ctx)
	if err != nil {
		return newData(selectedColumns.Names(), fsMetadata.columns.names(), nil, err)
	}

	return newData(selectedColumns.Names(), fsMetadata.columns.names(), res, nil)
}

func New() DB {
	return &db{files: make(map[string]fileMetadata)}
}

func createConditionColumnMetadata(fsMetadata fileMetadata) conditionResolver.ColumnMetadata {
	positions := make([]int, len(fsMetadata.columns))
	columnNames := make([]string, len(fsMetadata.columns))

	for i, m := range fsMetadata.columns {
		positions[i] = m.position
		columnNames[i] = m.name
	}

	return conditionResolver.NewColumnMetadata(positions, columnNames)
}

func createSelectedColumnMetadata(structure syntax.Structure, fsMetadata fileMetadata) selectedColumnMetadata.ColumnMetadata {
	return selectedColumnMetadata.New(structure.Column().Columns(), fsMetadata.columns.names())
}

func newData(selected, all []string, data []map[string]string, err error) Data {
	return Data{
		SelectedColumns: selected,
		AllColumns:      all,
		Error:           err,
		Data:            data,
	}
}
