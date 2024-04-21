package job

import (
	"context"
	"github.com/MarioLegenda/cig/pkg"
)

type SearchResult = []map[string]string
type JobFn = func(id int, writer chan pkg.Result[SearchResult], ctx context.Context)
