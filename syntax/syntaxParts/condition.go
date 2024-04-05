package syntaxParts

type condition struct {
	value        string
	column       string
	operator     string
	compoundHead Condition
	next         Condition
	prev         Condition
}

type Condition interface {
	Value() string
	Next() Condition
	Prev() Condition
	SetNext(item Condition)
	SetPrev(item Condition)
	Column() string
	Operator() string
}

func (i *condition) Value() string {
	return i.value
}

func (i *condition) Next() Condition {
	return i.next
}

func (i *condition) Prev() Condition {
	return i.prev
}

func (i *condition) SetNext(item Condition) {
	i.next = item
}

func (i *condition) SetPrev(item Condition) {
	i.prev = item
}

func (i *condition) Column() string {
	return i.column
}

func (i *condition) Operator() string {
	return i.operator
}

func NewCondition(column, operator, value string) Condition {
	return &condition{
		value:    value,
		column:   column,
		operator: operator,
		next:     nil,
		prev:     nil,
	}
}
