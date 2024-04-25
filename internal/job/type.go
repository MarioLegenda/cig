package job

import (
	"context"
)

type SearchResult = []map[string]string
type SearchFn = func(id int, ctx context.Context) (SearchResult, error)
