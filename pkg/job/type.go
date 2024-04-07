package job

import (
	"context"
	"github.com/MarioLegenda/cig/pkg/result"
)

type SearchResult = []map[string]string
type JobFn = func(id int, writer chan result.Result[SearchResult], ctx context.Context)
