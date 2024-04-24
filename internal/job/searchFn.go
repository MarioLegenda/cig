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

func SearchFactory(selectedColumns selectedColumnMetadata.ColumnMetadata, metadata conditionResolver.ColumnMetadata, condition syntaxStructure.Condition, constraints syntaxStructure.StructureConstraints, f io.ReadCloser) SearchFn {
	return func(id int, ctx context.Context) pkg.Result[SearchResult] {
		results := make(SearchResult, 0)
		lineReader := fs.NewLineReader(f, true)
		limit := constraints.Limit()
		offset := constraints.Offset()
		orderBy := constraints.OrderBy()

		var currentCollectedLimit int64
		var currentCollectedOffset int64

		for {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					return pkg.NewResult[SearchResult](results, nil)
				}
			default:
				lines, err := lineReader()
				if err != nil {
					return pkg.NewResult[SearchResult](nil, []error{
						fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
					})
				}

				if len(lines) == 0 {
					if orderBy != nil {
						sortResults(results, orderBy)
					}

					return pkg.NewResult[SearchResult](results, nil)
				}

				if offset != nil && currentCollectedOffset < offset.Value() {
					currentCollectedOffset++

					continue
				}

				if limit != nil && currentCollectedLimit == limit.Value() {
					if orderBy != nil {
						sortResults(results, orderBy)
					}

					return pkg.NewResult[SearchResult](results, nil)
				}

				if condition != nil {
					ok, err := conditionResolver.ResolveCondition(condition, metadata, lines)
					if err != nil {
						return pkg.NewResult[SearchResult](nil, []error{
							fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
						})
					}

					if ok {
						res, err := createResult(lines, selectedColumns)
						if err != nil {
							return pkg.NewResult[SearchResult](nil, []error{
								fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
							})
						}

						if limit != nil {
							currentCollectedLimit++
						}

						results = append(results, res)
					}
				} else {
					res, err := createResult(lines, selectedColumns)
					if err != nil {
						return pkg.NewResult[SearchResult](nil, []error{
							fmt.Errorf("Error in job %d while reading from the file: %w", id, err),
						})
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
