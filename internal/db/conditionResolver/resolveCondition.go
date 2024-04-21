package conditionResolver

import (
	"fmt"
	"github.com/MarioLegenda/cig/internal/db/comparison"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"github.com/MarioLegenda/cig/internal/syntax/syntaxStructure"
)

type cond struct {
	incomingValue  string
	toCompareValue string
	op             string
	dataType       string
	result         bool
}

// good enough for now, technically incorrect
func ResolveCondition(condition syntaxStructure.Condition, metadata ColumnMetadata, lines []string) (bool, error) {
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
					dataType:       head.Column().DataType(),
					incomingValue:  head.Value().Value(),
					op:             head.Operator().ConditionType(),
				})
				prevOp = operators.AndOperator
			} else if next.Operator().ConditionType() == operators.OrOperator {
				ors = append(ors, cond{
					toCompareValue: lines[p],
					dataType:       head.Column().DataType(),
					incomingValue:  head.Value().Value(),
					op:             head.Operator().ConditionType(),
				})
				prevOp = operators.OrOperator
			}

			// skip operator
			head = head.Next().Next()
			// is this the last item?
		} else if next == nil {
			if prevOp == "" {
				ands = append(ands, cond{
					toCompareValue: lines[p],
					dataType:       head.Column().DataType(),
					incomingValue:  head.Value().Value(),
					op:             head.Operator().ConditionType(),
				})
			}

			if prevOp == operators.AndOperator {
				ands = append(ands, cond{
					toCompareValue: lines[p],
					dataType:       head.Column().DataType(),
					incomingValue:  head.Value().Value(),
					op:             head.Operator().ConditionType(),
				})
			}

			if prevOp == operators.OrOperator {
				ors = append(ors, cond{
					toCompareValue: lines[p],
					dataType:       head.Column().DataType(),
					incomingValue:  head.Value().Value(),
					op:             head.Operator().ConditionType(),
				})
			}

			break
		}
	}

	if len(ors) == 0 {
		processables := make([]comparison.Processor, len(ands))
		for i, t := range ands {
			processables[i] = comparison.NewProcessable(t.incomingValue, t.toCompareValue, t.op, t.dataType)
		}

		processor := comparison.NewProcessor(processables)
		ok, err := processor.Process()
		if !ok || err != nil {
			return ok, err
		}

		return true, nil
	}

	if len(ors) != 0 {
		for _, t := range ors {
			processable := comparison.NewProcessable(t.incomingValue, t.toCompareValue, t.op, t.dataType)
			ok, err := processable.Process()
			if err != nil {
				return false, err
			}

			if ok {
				return true, nil
			}
		}
	}

	return false, nil
}
