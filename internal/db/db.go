package db

import (
	"context"
	"github.com/MarioLegenda/cig/internal/db/conditionResolver"
	"github.com/MarioLegenda/cig/internal/db/selectedColumnMetadata"
	job2 "github.com/MarioLegenda/cig/internal/job"
	"github.com/MarioLegenda/cig/internal/syntax"
	"github.com/MarioLegenda/cig/pkg"
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
	Run(s syntax.Structure) (job2.SearchResult, error)
}

func (d *db) Run(s syntax.Structure) (job2.SearchResult, error) {
	file := s.FileDB()

	fileHandler, err := prepareRun(file, d)
	if err != nil {
		return nil, err
	}

	conditionColumnMetadata := createConditionColumnMetadata(d.files[file.Alias()])
	selectedColumns := createSelectedColumnMetadata(s, d.files[file.Alias()])

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if s.Condition() != nil {
		return job2.SearchFactory(selectedColumns, conditionColumnMetadata, s.Condition(), s.Constraints(), fileHandler)(0, ctx)
	}

	return job2.SearchFactory(selectedColumns, conditionColumnMetadata, s.Condition(), s.Constraints(), fileHandler)(0, ctx)
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
	return selectedColumnMetadata.New(structure.Column().Columns(), fsMetadata.columns.Names())
}
