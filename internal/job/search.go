package job

import (
	"context"
	"fmt"
	"github.com/MarioLegenda/cig/internal/db/conditionResolver"
	"github.com/MarioLegenda/cig/internal/db/fs"
	"github.com/MarioLegenda/cig/internal/db/selectedColumnMetadata"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
	"github.com/MarioLegenda/cig/pkg"
	"io"
)

func Search(selectedColumns selectedColumnMetadata.ColumnMetadata, metadata conditionResolver.ColumnMetadata, condition syntaxStructure.Condition, constraints syntaxStructure.StructureConstraints, f io.ReadCloser) JobFn {
	return func(id int, writer chan pkg.Result[SearchResult], ctx context.Context) {
		results := make(SearchResult, 0)
		lineReader := fs.NewLineReader(f, true)
		limit := constraints.Limit()
		offset := constraints.Offset()

		var currentCollectedLimit int64
		var currentCollectedOffset int64

		for {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					writer <- pkg.NewResult[SearchResult](nil, []error{
						fmt.Errorf("Deadline exceeded for job %d: %w", id, ctx.Err()),
					})
				}

				return
			default:
				lines, err := lineReader()
				if err != nil {
					writer <- pkg.NewResult[SearchResult](nil, []error{
						fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
					})
					return
				}

				if len(lines) == 0 {
					writer <- pkg.NewResult[SearchResult](results, nil)

					return
				}

				if offset != nil && currentCollectedOffset < offset.Value() {
					currentCollectedOffset++

					continue
				}

				if limit != nil && currentCollectedLimit == limit.Value() {
					writer <- pkg.NewResult[SearchResult](results, nil)
					return
				}

				if condition != nil {
					ok, err := conditionResolver.ResolveCondition(condition, metadata, lines)
					if err != nil {
						writer <- pkg.NewResult[SearchResult](nil, []error{
							fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
						})
						return
					}

					if ok {
						res, err := createResult(lines, selectedColumns)
						if err != nil {
							writer <- pkg.NewResult[SearchResult](nil, []error{
								fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
							})

							return
						}

						if limit != nil {
							currentCollectedLimit++
						}

						results = append(results, res)
					}
				} else {
					res, err := createResult(lines, selectedColumns)
					if err != nil {
						writer <- pkg.NewResult[SearchResult](nil, []error{
							fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
						})

						return
					}

					if limit != nil {
						currentCollectedLimit++
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
