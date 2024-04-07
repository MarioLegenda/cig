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

func NewColumnMetadata(columnsToReturn []int, columnNames []string) columnMetadata {
	return columnMetadata{
		columnsToReturn: columnsToReturn,
		columnNames:     columnNames,
	}
}

func Search(columnPosition int, metadata columnMetadata, value string, f io.ReadCloser) JobFn {
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
					fmt.Println("sent error", err)
					writer <- result.NewResult[SearchResult](nil, []error{
						fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
					})
					return
				}

				if len(lines) == 0 {
					return
				}

				if columnPosition == -1 {
					singleResult := make(map[string]string)

					for _, line := range lines {
						for _, c := range metadata.columnsToReturn {
							columnName := metadata.columnNames[c]
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
