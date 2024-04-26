package job

import (
	"github.com/MarioLegenda/cig/internal/db/conditionResolver"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
	"sort"
	"strconv"
)

type comparableConstraint interface {
	int64 | float64 | string
}

type By func(p1, p2 []string) bool

type resultSorter struct {
	results [][]string
	by      By
}

func (s *resultSorter) Len() int {
	return len(s.results)
}

func (s *resultSorter) Swap(i, j int) {
	s.results[i], s.results[j] = s.results[j], s.results[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *resultSorter) Less(i, j int) bool {
	return s.by(s.results[i], s.results[j])
}

func (by By) Sort(results [][]string) {
	ps := &resultSorter{
		results: results,
		by:      by,
	}

	sort.Sort(ps)
}

func sortResults(result [][]string, orderBy syntaxStructure.OrderBy, metadata conditionResolver.ColumnMetadata) [][]string {
	orderByColumns := orderBy.Columns()
	direction := orderBy.Direction()
	for _, c := range orderByColumns {
		currentPosition := metadata.Position(c.Column())

		fn := func(p1, p2 []string) bool {
			v1int, p1IntErr := strconv.ParseInt(p1[currentPosition], 10, 64)
			v2int, p2IntErr := strconv.ParseInt(p2[currentPosition], 10, 64)

			if p1IntErr != nil && p2IntErr != nil {
				if direction == operators.Desc {
					return v1int > v2int
				}

				return v1int < v2int
			}

			v1float, p1FloatErr := strconv.ParseFloat(p1[currentPosition], 64)
			v2float, p2FloatErr := strconv.ParseFloat(p2[currentPosition], 64)

			if p1FloatErr != nil && p2FloatErr != nil {
				if direction == operators.Desc {
					return v1float > v2float
				}

				return v1float < v2float
			}

			if direction == operators.Desc {
				return p1[currentPosition] > p2[currentPosition]
			}

			return p1[currentPosition] < p2[currentPosition]
		}

		By(fn).Sort(result)
	}

	return result
}

/*func getValue[T int64 | float64 | string](p1 string, p2 string) (T, T) {
	v1i, errV1i := strconv.ParseInt(p1, 10, 64)
	v2i, errV2i := strconv.ParseInt(p2, 10, 64)

	if errV1i != nil && errV2i != nil {
		return T(v1i), T(v2i)
	}

	v1f, errV1f := strconv.ParseFloat(p1, 64)
	v2f, errV2f := strconv.ParseFloat(p2, 64)

	if errV1f != nil && errV2f != nil {
		return T(v1f), T(v2f)
	}

	return T(p1), T(p2)
}
*/
