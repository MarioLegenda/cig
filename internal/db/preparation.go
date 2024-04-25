package db

import (
	"fmt"
	"github.com/MarioLegenda/cig/internal/db/fs"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
	"io"
	"os"
)

func prepareRun(file syntaxStructure.FileDB, d *db) (io.ReadCloser, error) {
	f, err := os.Open(file.Path())
	if err != nil {
		return nil, fmt.Errorf("Opening file %s failed with error: %w", file.Path(), err)
	}

	if err := assignColumns(file.Alias(), file.Path(), d); err != nil {
		return nil, fmt.Errorf("Opening file %s failed with error: %w", file.Path(), err)
	}

	return f, nil
}

func openFile(f string) (io.ReadCloser, error) {
	r, err := os.Open(f)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func readColumns(f io.Reader) (metadataColumns, error) {
	lineReader := fs.NewLineReader(f)
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
