package db

import (
	"fmt"
	"github.com/MarioLegenda/cig/pkg/db/fs"
	"github.com/MarioLegenda/cig/pkg/result"
	"github.com/MarioLegenda/cig/pkg/syntax"
	"io"
	"os"
)

type fileMetadata struct {
	columns      []string
	originalPath string
	file         io.ReadCloser
}

type db struct {
	files map[string]fileMetadata
}

type DB interface {
	Run(s syntax.Structure) result.Result[map[string]string]
	Close() result.Result[any]
}

func (d *db) Run(s syntax.Structure) result.Result[map[string]string] {
	file := s.FileDB()
	errs := make([]error, 0)

	if err := openFiles(file.Alias(), file.Path(), d); err != nil {
		errs = append(errs, fmt.Errorf("Opening file %s failed with error: %w", file.Path(), err))
		return result.NewResult[map[string]string](nil, errs)
	}

	if s.Condition() != nil {
	}

	return result.NewResult[map[string]string](nil, nil)
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

func readColumns(f io.Reader) ([]string, error) {
	b, err := fs.ReadLine(f)
	if err != nil {
		return nil, err
	}

	columns := make([]string, 0)
	lastIndexMarked := 0
	for i, k := range b {
		if k == 44 {
			columns = append(columns, string(b[lastIndexMarked:i]))
			lastIndexMarked = i + 1
		}
	}

	return columns, nil
}
