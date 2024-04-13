package job

import (
	"context"
	"fmt"
	"github.com/MarioLegenda/cig/internal/db/conditionResolver"
	"github.com/MarioLegenda/cig/internal/db/fs"
	"github.com/MarioLegenda/cig/internal/db/selectedColumnMetadata"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
	"github.com/MarioLegenda/cig/pkg/result"
	"io"
)

func Search(selectedColumns selectedColumnMetadata.ColumnMetadata, metadata conditionResolver.ColumnMetadata, condition syntaxStructure.Condition, f io.ReadCloser) JobFn {
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
						res, err := createResult(lines, selectedColumns)
						if err != nil {
							writer <- result.NewResult[SearchResult](nil, []error{
								fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
							})

							return
						}

						results = append(results, res)
					}
				} else {
					res, err := createResult(lines, selectedColumns)
					if err != nil {
						writer <- result.NewResult[SearchResult](nil, []error{
							fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
						})

						return
					}

					results = append(results, res)
				}
			}
		}
	}
}

func createResult(lines []string, selectedColumns selectedColumnMetadata.ColumnMetadata) (map[string]string, error) {
	res := make(map[string]string)
	for linePosition, line := range lines {
		if selectedColumns.HasPosition(linePosition) {
			columnName := selectedColumns.Column(linePosition)
			if columnName == "" {
				return nil, fmt.Errorf("Column not found for position %d. This should not happen and is a bug", linePosition)
			}
			res[columnName] = line
		}
	}

	return res, nil
}
