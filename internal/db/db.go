package db

import (
	"context"
	"github.com/MarioLegenda/cig/internal/db/conditionResolver"
	"github.com/MarioLegenda/cig/internal/db/selectedColumnMetadata"
	job2 "github.com/MarioLegenda/cig/internal/job"
	"github.com/MarioLegenda/cig/internal/scheduler"
	"github.com/MarioLegenda/cig/internal/syntax"
	"github.com/MarioLegenda/cig/pkg/result"
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
	Run(s syntax.Structure) result.Result[job2.SearchResult]
	Close() result.Result[any]
}

func (d *db) Run(s syntax.Structure) result.Result[job2.SearchResult] {
	file := s.FileDB()
	errs := make([]error, 0)

	fileHandler, err := prepareRun(file, d)
	if err != nil {
		errs = append(errs, err)
		return result.NewResult[job2.SearchResult](nil, errs)
	}

	conditionColumnMetadata := createConditionColumnMetadata(d.files[file.Alias()])
	selectedColumns := createSelectedColumnMetadata(s, d.files[file.Alias()])

	jobId := 0
	workerScheduler := scheduler.New()

	if s.Condition() != nil {
		if err := workerScheduler.Schedule(jobId); err != nil {
			errs = append(errs, err)
			return result.NewResult[job2.SearchResult](nil, errs)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		workerScheduler.Send(
			jobId,
			job2.Search(
				selectedColumns,
				conditionColumnMetadata,
				s.Condition(),
				fileHandler,
			),
			ctx,
		)
	} else {
		if err := workerScheduler.Schedule(jobId); err != nil {
			errs = append(errs, err)
			return result.NewResult[job2.SearchResult](nil, errs)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		workerScheduler.Send(
			jobId,
			job2.Search(
				selectedColumns,
				conditionColumnMetadata,
				nil,
				fileHandler,
			),
			ctx,
		)
	}

	if err := workerScheduler.Start(); err != nil {
		errs = append(errs, err)
		return result.NewResult[job2.SearchResult](nil, errs)
	}

	processedResults, resErrs := processResults(workerScheduler.Results())
	if resErrs != nil {
		errs = append(errs, resErrs...)
		return result.NewResult[job2.SearchResult](nil, errs)
	}

	workerScheduler.Close()

	return result.NewResult[job2.SearchResult](processedResults, nil)
}

func (d *db) Close() result.Result[any] {
	return result.NewResult[any](nil, nil)
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

func processResults(schedulerResults []result.Result[job2.SearchResult]) (job2.SearchResult, []error) {
	newResults := make(job2.SearchResult, 0)
	for _, res := range schedulerResults {
		if res.HasErrors() {
			return nil, res.Errors()
		}

		wrappedResults := res.Result()
		for _, r := range wrappedResults {
			newResults = append(newResults, r)
		}
	}

	return newResults, nil
}
