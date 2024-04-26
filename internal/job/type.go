package job

import (
	"context"
)

type SearchResult []map[string]string
type JobFn = func(id int, writer chan SearchResult, ctx context.Context)
type SearchFn = func(id int, ctx context.Context) (SearchResult, error)
