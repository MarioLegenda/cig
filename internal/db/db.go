package db

import (
	"context"
	"fmt"
	"github.com/MarioLegenda/cig/internal/db/conditionResolver"
	"github.com/MarioLegenda/cig/internal/db/fs"
	job2 "github.com/MarioLegenda/cig/internal/job"
	"github.com/MarioLegenda/cig/internal/scheduler"
	"github.com/MarioLegenda/cig/internal/syntax"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxParts"
	"github.com/MarioLegenda/cig/pkg/result"
	"io"
	"os"
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

	jobColumnMetadata := createJobColumnMetadata(d.files[file.Alias()])

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
				-1,
				jobColumnMetadata,
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
				-1,
				jobColumnMetadata,
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

func assignColumns(alias, f string, d *db) error {
	if _, ok := d.files[alias]; ok {
		return nil
	}

	r, err := openFile(f)
	if err != nil {
		return err
	}
	defer r.Close()

	columns, err := readColumns(r)
	if err != nil {
		return err
	}

	d.files[alias] = fileMetadata{
		columns:      columns,
		originalPath: f,
	}

	return nil
}

func openFile(f string) (io.ReadCloser, error) {
	r, err := os.Open(f)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func readColumns(f io.Reader) (metadataColumns, error) {
	lineReader := fs.NewLineReader(f, false)
	cls, err := lineReader()
	if err != nil {
		return nil, err
	}

	columns := make(metadataColumns, 0)
	for i, k := range cls {
		columns = append(columns, metadataColumn{
			position: i,
			name:     k,
		})
	}

	return columns, nil
}

func createJobColumnMetadata(fsMetadata fileMetadata) conditionResolver.ColumnMetadata {
	positions := make([]int, len(fsMetadata.columns))
	columnNames := make([]string, len(fsMetadata.columns))

	for _, m := range fsMetadata.columns {
		positions = append(positions, m.position)
		columnNames = append(columnNames, m.name)
	}

	return conditionResolver.NewColumnMetadata(positions, columnNames)
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

func prepareRun(file syntaxParts.FileDB, d *db) (io.ReadCloser, error) {
	f, err := os.Open(file.Path())
	if err != nil {
		return nil, fmt.Errorf("Opening file %s failed with error: %w", file.Path(), err)
	}

	if err := assignColumns(file.Alias(), file.Path(), d); err != nil {
		return nil, fmt.Errorf("Opening file %s failed with error: %w", file.Path(), err)
	}

	return f, nil
}
