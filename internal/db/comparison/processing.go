package comparison

import (
	"fmt"
	"github.com/MarioLegenda/cig/internal/syntax/dataTypes"
	"github.com/MarioLegenda/cig/internal/syntax/operators"
	"strconv"
)

type Processor interface {
	Process() (bool, error)
}

type processor struct {
	processables []Processor
}

type processable struct {
	incomingValue  string
	conditionValue string
	op             string
	dataType       string
}

func (p processable) Process() (bool, error) {
	if p.op == operators.EqualOperator {
		return compareEqual(p.dataType, p.incomingValue, p.conditionValue)
	} else if p.op == operators.UnEqualOperator {
		return compareUnequal(p.dataType, p.incomingValue, p.conditionValue)
	} else if p.op == operators.LessThanOperator {
		return compareLessThan(p.dataType, p.incomingValue, p.conditionValue)
	} else if p.op == operators.LessThanOrEqualOperator {
		return compareLessThanOrEqual(p.dataType, p.incomingValue, p.conditionValue)
	} else if p.op == operators.GreaterThanOperator {
		return compareGreaterThan(p.dataType, p.incomingValue, p.conditionValue)
	} else if p.op == operators.GreaterThanOrEqualOperator {
		return compareGreaterThanOrEqual(p.dataType, p.incomingValue, p.conditionValue)
	}

	return false, fmt.Errorf("Internal error. Could not match condition operator %s with any of valid operators", p.op)
}

func (p processor) Process() (bool, error) {
	for _, k := range p.processables {
		ok, _ := k.Process()
		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func NewProcessor(processables []Processor) Processor {
	return processor{processables: processables}
}

func NewProcessable(conditionValue, incomingValue, op, dataType string) Processor {
	return processable{
		incomingValue:  incomingValue,
		conditionValue: conditionValue,
		op:             op,
		dataType:       dataType,
	}
}

/*
*
Leave all of this. comparable does not work as a variable data type so cannot be used.
Converting to interface{} will degrade performance (not that big of an issue??).
This is more maintainable.
*/
func compareEqual(dt, incomingValue, conditionValue string) (bool, error) {
	if dt == dataTypes.Int {
		a, err := strconv.ParseInt(incomingValue, 10, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseInt(conditionValue, 10, 64)
		if err != nil {
			return false, err
		}

		return a == b, nil
	}

	if dt == dataTypes.Float {
		a, err := strconv.ParseFloat(incomingValue, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseFloat(conditionValue, 64)
		if err != nil {
			return false, err
		}

		return a == b, nil
	}

	return incomingValue == conditionValue, nil
}

func compareUnequal(dt, incomingValue, conditionValue string) (bool, error) {
	if dt == dataTypes.Int {
		a, err := strconv.ParseInt(incomingValue, 10, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseInt(conditionValue, 10, 64)
		if err != nil {
			return false, err
		}

		return a != b, nil
	}

	if dt == dataTypes.Float {
		a, err := strconv.ParseFloat(incomingValue, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseFloat(conditionValue, 64)
		if err != nil {
			return false, err
		}

		return a != b, nil
	}

	return incomingValue != conditionValue, nil
}

func compareLessThan(dt, incomingValue, conditionValue string) (bool, error) {
	if dt == dataTypes.Int {
		a, err := strconv.ParseInt(incomingValue, 10, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseInt(conditionValue, 10, 64)
		if err != nil {
			return false, err
		}

		return a < b, nil
	}

	if dt == dataTypes.Float {
		a, err := strconv.ParseFloat(incomingValue, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseFloat(conditionValue, 64)
		if err != nil {
			return false, err
		}

		return a < b, nil
	}

	return incomingValue < conditionValue, nil
}

func compareLessThanOrEqual(dt, incomingValue, conditionValue string) (bool, error) {
	if dt == dataTypes.Int {
		a, err := strconv.ParseInt(incomingValue, 10, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseInt(conditionValue, 10, 64)
		if err != nil {
			return false, err
		}

		return a <= b, nil
	}

	if dt == dataTypes.Float {
		a, err := strconv.ParseFloat(incomingValue, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseFloat(conditionValue, 64)
		if err != nil {
			return false, err
		}

		return a <= b, nil
	}

	return incomingValue <= conditionValue, nil
}

func compareGreaterThan(dt, incomingValue, conditionValue string) (bool, error) {
	if dt == dataTypes.Int {
		a, err := strconv.ParseInt(incomingValue, 10, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseInt(conditionValue, 10, 64)
		if err != nil {
			return false, err
		}

		return a > b, nil
	}

	if dt == dataTypes.Float {
		a, err := strconv.ParseFloat(incomingValue, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseFloat(conditionValue, 64)
		if err != nil {
			return false, err
		}

		return a > b, nil
	}

	return incomingValue > conditionValue, nil
}

func compareGreaterThanOrEqual(dt, incomingValue, conditionValue string) (bool, error) {
	if dt == dataTypes.Int {
		a, err := strconv.ParseInt(incomingValue, 10, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseInt(conditionValue, 10, 64)
		if err != nil {
			return false, err
		}

		return a >= b, nil
	}

	if dt == dataTypes.Float {
		a, err := strconv.ParseFloat(incomingValue, 64)
		if err != nil {
			return false, err
		}

		b, err := strconv.ParseFloat(conditionValue, 64)
		if err != nil {
			return false, err
		}

		return a >= b, nil
	}

	return incomingValue >= conditionValue, nil
}
