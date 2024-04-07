package db

import (
	"context"
	"fmt"
	"github.com/MarioLegenda/cig/pkg/db/fs"
	job2 "github.com/MarioLegenda/cig/pkg/job"
	"github.com/MarioLegenda/cig/pkg/result"
	"github.com/MarioLegenda/cig/pkg/scheduler"
	"github.com/MarioLegenda/cig/pkg/syntax"
	"io"
	"os"
	"time"
)

type metadataColumn struct {
	position int
	name     string
}

type fileMetadata struct {
	columns      []metadataColumn
	originalPath string
	file         io.ReadCloser
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

	if err := openFiles(file.Alias(), file.Path(), d); err != nil {
		errs = append(errs, fmt.Errorf("Opening file %s failed with error: %w", file.Path(), err))
		return result.NewResult[job2.SearchResult](nil, errs)
	}

	metadata := d.files[file.Alias()]

	positions := make([]int, len(metadata.columns))
	columnNames := make([]string, len(metadata.columns))

	for _, m := range metadata.columns {
		positions = append(positions, m.position)
		columnNames = append(columnNames, m.name)
	}

	jobMetadata := job2.NewColumnMetadata(positions, columnNames)

	workerScheduler := scheduler.New()
	if err := workerScheduler.Schedule(0); err != nil {
		errs = append(errs, err)
		return result.NewResult[job2.SearchResult](nil, errs)
	}

	workerScheduler.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 128*time.Second)
	defer cancel()
	workerScheduler.Send(0, job2.Search(-1, jobMetadata, "", metadata.file), ctx)

	results := workerScheduler.Results()

	newResults := make(job2.SearchResult, 0)
	for _, res := range results {
		if res.HasErrors() {
			errs = append(errs, res.Errors()...)
			return result.NewResult[job2.SearchResult](nil, errs)
		}

		wrappedResults := res.Result()
		for _, r := range wrappedResults {
			newResults = append(newResults, r)
		}
	}

	workerScheduler.Close()

	return result.NewResult[job2.SearchResult](newResults, nil)
}

func (d *db) Close() result.Result[any] {
	errs := make([]error, 0)
	for _, v := range d.files {
		err := v.file.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("File %s could not be closed without an error: %w", v.originalPath, err))
		}
	}

	return result.NewResult[any](nil, errs)
}

func New() DB {
	return &db{files: make(map[string]fileMetadata)}
}

func openFiles(alias, f string, d *db) error {
	if _, ok := d.files[alias]; ok {
		return nil
	}

	r, err := os.Open(f)
	if err != nil {
		return err
	}

	columns, err := readColumns(r)
	if err != nil {
		return err
	}

	d.files[alias] = fileMetadata{
		columns:      columns,
		originalPath: f,
		file:         r,
	}

	return nil
}

func readColumns(f io.Reader) ([]metadataColumn, error) {
	lineReader := fs.NewLineReader(f, false)
	cls, err := lineReader()
	if err != nil {
		return nil, err
	}

	columns := make([]metadataColumn, 0)
	for i, k := range cls {
		columns = append(columns, metadataColumn{
			position: i,
			name:     k,
		})
	}

	return columns, nil
}
