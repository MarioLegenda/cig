package job

import (
	"context"
	"fmt"
	"github.com/MarioLegenda/cig/internal/db/conditionResolver"
	"github.com/MarioLegenda/cig/internal/db/fs"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxParts"
	"github.com/MarioLegenda/cig/pkg/result"
	"io"
)

func Search(columnPosition int, metadata conditionResolver.ColumnMetadata, condition syntaxParts.Condition, f io.ReadCloser) JobFn {
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

				if condition != nil {
					ok, err := conditionResolver.ResolveCondition(condition, metadata, lines)
					if err != nil {
						writer <- result.NewResult[SearchResult](nil, []error{
							fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
						})
						return
					}

					if ok {
						results = append(results, createResult(lines, metadata))
					}
				} else {
					results = append(results, createResult(lines, metadata))
				}
			}
		}
	}
}

func createResult(lines []string, metadata conditionResolver.ColumnMetadata) map[string]string {
	res := make(map[string]string)
	for _, line := range lines {
		for _, c := range metadata.ColumnsToReturn() {
			columnName := metadata.ColumnNames()[c]
			res[columnName] = line
		}
	}

	return res
}
