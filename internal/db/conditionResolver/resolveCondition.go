package conditionResolver

import (
	"fmt"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxParts"
)

type value struct {
	value     string
	convertTo string
}

type cond struct {
	incomingValue  string
	toCompareValue string
	op             string
	result         bool
}

type Value interface {
	Value() string
	ConvertTo() string
}

func (v value) Value() string {
	return v.value
}

// TODO: convert to other primitive data types here, not yet implemented
func (v value) ConvertTo() string {
	return ""
}

// good enough for now
func ResolveCondition(condition syntaxParts.Condition, metadata ColumnMetadata, lines []string) (bool, error) {
	ands := make([]cond, 0)
	ors := make([]cond, 0)

	head := condition
	var prevOp string
	// setup
	for head != nil {
		next := head.Next()
		p := metadata.Position(head.Column().Column())
		if p == -1 {
			return false, fmt.Errorf("Invalid column to compare. Column %s not found", head.Column().Column())
		}

		if next != nil {
			if next.Operator().ConditionType() == operators.AndOperator {
				ands = append(ands, cond{
					toCompareValue: lines[p],
					incomingValue:  head.Value().Value(),
					op:             head.Operator().ConditionType(),
				})
			} else if next.Operator().ConditionType() == operators.OrOperator {
				ands = append(ors, cond{
					toCompareValue: lines[p],
					incomingValue:  head.Value().Value(),
					op:             head.Operator().Original(),
				})
			}

			prevOp = head.Operator().ConditionType()
			// skip operator
			head = head.Next().Next()
			// is this the last item?
		} else if next == nil {
			if prevOp == operators.AndOperator {
				ands = append(ands, cond{
					toCompareValue: lines[p],
					incomingValue:  head.Value().Value(),
					op:             head.Operator().ConditionType(),
				})
			}

			if prevOp == operators.OrOperator {
				ands = append(ands, cond{
					toCompareValue: lines[p],
					incomingValue:  head.Value().Value(),
					op:             head.Operator().ConditionType(),
				})
			}

			break
		}
	}

	for _, t := range ands {
		if t.op == operators.EqualOperator && t.incomingValue == t.toCompareValue {
			t.result = true
		} else if t.op == operators.UnEqualOperator && t.incomingValue != t.toCompareValue {
			t.result = true
		}
	}

	if len(ors) == 0 {
		for _, t := range ands {
			if !t.result {
				return false, nil
			}
		}

		return true, nil
	}

	if len(ors) == 0 {
		for _, t := range ors {
			if t.op == operators.EqualOperator && t.incomingValue == t.toCompareValue {
				return true, nil
			} else if t.op == operators.UnEqualOperator && t.incomingValue != t.toCompareValue {
				return true, nil
			}
		}
	}

	return false, nil
}
