package db

import (
	"fmt"
	"github.com/MarioLegenda/cig/internal/db/fs"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
	"io"
	"os"
)

func prepareRun(file syntaxStructure.FileDB, d *db) (io.ReadCloser, error) {
	f, err := assignColumns(file.Path(), d)
	if err != nil {
		return nil, fmt.Errorf("Opening file %s failed with error: %w", file.Path(), err)
	}

	return f, nil
}

func openFile(f string) (*os.File, error) {
	r, err := os.Open(f)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func readColumns(f *os.File) (metadataColumns, error) {
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

func assignColumns(f string, d *db) (*os.File, error) {
	r, err := openFile(f)
	if err != nil {
		return nil, err
	}

	columns, err := readColumns(r)
	if err != nil {
		return nil, err
	}

	if _, err := r.Seek(0, 0); err != nil {
		return nil, err
	}

	d.openFs = r
	d.metadata = fileMetadata{
		columns:      columns,
		originalPath: f,
	}

	return r, nil
}
