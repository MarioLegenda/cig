package job

import (
	"context"
	"fmt"
	"github.com/MarioLegenda/cig/internal/db/fs"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
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

func Search(columnPosition int, metadata ColumnMetadata, op Operator, f io.ReadCloser) JobFn {
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

				if op != nil {
					lineValue := lines[op.Column()]

					if op.Operator() == operators.EqualOperator && lineValue == op.Value() {
						results = append(results, createResult(lines, metadata))
					}
				} else {
					results = append(results, createResult(lines, metadata))
				}
			}
		}

		writer <- result.NewResult[SearchResult](results, nil)
	}
}

func createResult(lines []string, metadata ColumnMetadata) map[string]string {
	res := make(map[string]string)
	for _, line := range lines {
		for _, c := range metadata.ColumnsToReturn() {
			columnName := metadata.ColumnNames()[c]
			res[columnName] = line
		}
	}

	return res
}
