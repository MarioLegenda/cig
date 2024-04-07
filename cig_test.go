package cig

import (
	"github.com/MarioLegenda/cig/pkg/job"
	"github.com/MarioLegenda/cig/pkg/result"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestGettingAllResults(t *testing.T) {
	c := New()

	res := c.Run("SELECT * FROM path:testdata/example.csv AS e")

	assert.False(t, res.HasErrors())
	assert.Equal(t, 0, len(res.Errors()))

	foundResults := res.Result()

	assert.Equal(t, 20858, len(foundResults))
}

func TestGettingResultsWithSingleWhereClause(t *testing.T) {
	c := New()

	res := c.Run("SELECT * FROM path:testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1'")

	assert.False(t, res.HasErrors())
	assert.Equal(t, 0, len(res.Errors()))

	foundResults := res.Result()

	assert.Equal(t, 2511, len(foundResults))
}

func TestParallelRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	c := New()

	results := make(chan result.Result[job.SearchResult], 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			results <- c.Run("SELECT * FROM path:testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1'")
			wg.Done()
		}()
	}

	wg.Wait()
	close(results)

	for res := range results {
		assert.Nil(t, res.Errors())
	}
}
