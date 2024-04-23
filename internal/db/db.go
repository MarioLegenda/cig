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
	Run(s syntax.Structure) pkg.Result[job2.SearchResult]
	Close() pkg.Result[any]
}

func (d *db) Run(s syntax.Structure) pkg.Result[job2.SearchResult] {
	file := s.FileDB()
	errs := make([]error, 0)

	fileHandler, err := prepareRun(file, d)
	if err != nil {
		errs = append(errs, err)
		return pkg.NewResult[job2.SearchResult](nil, errs)
	}

	conditionColumnMetadata := createConditionColumnMetadata(d.files[file.Alias()])
	selectedColumns := createSelectedColumnMetadata(s, d.files[file.Alias()])

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if s.Condition() != nil {
		fn := job2.SearchFactory(selectedColumns, conditionColumnMetadata, s.Condition(), s.Constraints(), fileHandler)
		return fn(0, ctx)
	}

	fn := job2.SearchFactory(selectedColumns, conditionColumnMetadata, s.Condition(), s.Constraints(), fileHandler)
	return fn(0, ctx)
}

func (d *db) Close() pkg.Result[any] {
	return pkg.NewResult[any](nil, nil)
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
