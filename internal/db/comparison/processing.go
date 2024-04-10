package comparison

import "github.com/MarioLegenda/cig/internal/syntax/operators"

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

// TODO: handle data type conversion and all the other stuff that comes with comparison operations
func (p processable) Process() (bool, error) {
	if p.op == operators.EqualOperator && p.incomingValue == p.conditionValue {
		return true, nil
	} else if p.op == operators.UnEqualOperator && p.incomingValue != p.conditionValue {
		return true, nil
	}

	return false, nil
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

func NewProcessable(incomingValue, conditionValue, op, dataType string) Processor {
	return processable{
		incomingValue:  incomingValue,
		conditionValue: conditionValue,
		op:             op,
		dataType:       dataType,
	}
}
