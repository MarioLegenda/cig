package job

type Operator interface {
	Value() string
	Operator() string
	Column() int
}

type operator struct {
	value    string
	operator string
	column   int
}

func (o operator) Value() string {
	return o.value
}

func (o operator) Operator() string {
	return o.operator
}

func (o operator) Column() int {
	return o.column
}

func NewOperator(value, op string, column int) Operator {
	return operator{
		column:   column,
		value:    value,
		operator: op,
	}
}
