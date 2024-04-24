package cig

import (
	"github.com/MarioLegenda/cig/internal/job"
	"github.com/MarioLegenda/cig/pkg"
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

	assert.Equal(t, 41716, len(foundResults))
}

func TestGettingResultsWithSingleWhereClause(t *testing.T) {
	c := New()

	res := c.Run("SELECT * FROM path:testdata/example.csv AS e WHERE 'e.Industry_aggregation_NZSIOC' = 'Level 1' OR 'e.Industry_aggregation_NZSIOC' = 'Level 2' AND 'e.Year'::int = '2021'")

	assert.False(t, res.HasErrors())
	assert.Equal(t, 0, len(res.Errors()))

	foundResults := res.Result()

	assert.Equal(t, 5031, len(foundResults))
}

func TestGettingResultsWithDataConversion(t *testing.T) {
	c := New()

	res := c.Run("SELECT * FROM path:testdata/example.csv AS e WHERE 'e.Year'::int > '2013'")

	assert.False(t, res.HasErrors())
	assert.Equal(t, 0, len(res.Errors()))

	foundResults := res.Result()

	assert.Equal(t, 37080, len(foundResults))
}

func TestGettingResultsOfASingleSelectedColumn(t *testing.T) {
	c := New()

	res := c.Run("SELECT 'e.Year' FROM path:testdata/example.csv AS e WHERE 'e.Year'::int > '2013'")

	assert.False(t, res.HasErrors())
	assert.Equal(t, 0, len(res.Errors()))

	foundResults := res.Result()

	for _, singleResult := range foundResults {
		assert.Equal(t, len(singleResult), 1)
		assert.Contains(t, singleResult, "Year")
	}

	assert.Equal(t, 37080, len(foundResults))
}

func TestGettingResultsOfMultipleSelectedColumn(t *testing.T) {
	c := New()

	res := c.Run("SELECT 'e.Year','e.Industry_aggregation_NZSIOC','e.Industry_code_NZSIOC' FROM path:testdata/example.csv AS e WHERE 'e.Year'::int > '2013'")

	assert.False(t, res.HasErrors())
	assert.Equal(t, 0, len(res.Errors()))

	foundResults := res.Result()

	for _, singleResult := range foundResults {
		assert.Equal(t, len(singleResult), 3)
		assert.Contains(t, singleResult, "Year")
		assert.Contains(t, singleResult, "Industry_aggregation_NZSIOC")
		assert.Contains(t, singleResult, "Industry_code_NZSIOC")

		assert.NotEmpty(t, singleResult["Year"])
		assert.NotEmpty(t, singleResult["Industry_aggregation_NZSIOC"])
		assert.NotEmpty(t, singleResult["Industry_code_NZSIOC"])
	}

	assert.Equal(t, 37080, len(foundResults))
}

func TestGettingResultsWithLimit(t *testing.T) {
	c := New()

	statements := []string{
		"SELECT 'e.Year' FROM path:testdata/example.csv AS e WHERE 'e.Year'::int > '2013' LIMIT 50",
		"SELECT 'e.Year' FROM path:testdata/example.csv AS e limit 50",
		"SELECT 'e.Year' FROM path:testdata/example.csv AS e offset 30 limit 50",
		"SELECT 'e.Year' FROM path:testdata/example.csv AS e offset 30 ORDER BY 'e.Year' limit 50",
		"SELECT 'e.Year' FROM path:testdata/example.csv AS e ORDER BY 'e.Year' offset 30  limit 50",
		"SELECT 'e.Year' FROM path:testdata/example.csv AS e limit     50     ORDER BY 'e.Year'   ",
	}

	for _, s := range statements {
		res := c.Run(s)

		assert.False(t, res.HasErrors())
		assert.Equal(t, 0, len(res.Errors()))

		foundResults := res.Result()

		assert.Equal(t, 50, len(foundResults))
	}
}

func TestGettingResultsWithOffset(t *testing.T) {
	c := New()

	res := c.Run("SELECT * FROM path:testdata/example.csv AS e OFFSET 10000")

	assert.False(t, res.HasErrors())
	assert.Equal(t, 0, len(res.Errors()))

	foundResults := res.Result()

	assert.Equal(t, 31716, len(foundResults))
}

func TestParallelRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	c := New()

	results := make(chan pkg.Result[job.SearchResult], 10)
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
