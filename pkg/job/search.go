package job

import (
	"context"
	"fmt"
	"github.com/MarioLegenda/cig/pkg/db/fs"
	"github.com/MarioLegenda/cig/pkg/result"
	"io"
)

type columnMetadata struct {
	columnsToReturn []int
	columnNames     []string
}

type ColumnMetadata interface {
	ColumnsToReturn() []int
	ColumnNames() []string
}

func NewColumnMetadata(columnsToReturn []int, columnNames []string) ColumnMetadata {
	return columnMetadata{
		columnsToReturn: columnsToReturn,
		columnNames:     columnNames,
	}
}

func (cm columnMetadata) ColumnsToReturn() []int {
	return cm.columnsToReturn
}

func (cm columnMetadata) ColumnNames() []string {
	return cm.columnNames
}

func Search(columnPosition int, metadata ColumnMetadata, value string, f io.ReadCloser) JobFn {
	return func(id int, writer chan result.Result[SearchResult], ctx context.Context) {
		results := make(SearchResult, 0)
		lineReader := fs.NewLineReader(f, true)

		for {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					writer <- result.NewResult[SearchResult](nil, []error{
						fmt.Errorf("Deadline exceeded for job %d: %w", id, ctx.Err()),
					})
				}

				return
			default:
				lines, err := lineReader()
				if err != nil {
					writer <- result.NewResult[SearchResult](nil, []error{
						fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
					})
					return
				}

				if len(lines) == 0 {
					writer <- result.NewResult[SearchResult](results, nil)

					return
				}

				if columnPosition == -1 {
					singleResult := make(map[string]string)

					for _, line := range lines {
						for _, c := range metadata.ColumnsToReturn() {
							columnName := metadata.ColumnNames()[c]
							singleResult[columnName] = line
						}
					}

					results = append(results, singleResult)
				}
			}
		}

		writer <- result.NewResult[SearchResult](results, nil)
	}
}
