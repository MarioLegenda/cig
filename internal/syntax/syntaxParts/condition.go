package syntaxParts

type Condition interface {
	Value() ConditionValue
	Next() Condition
	Prev() Condition
	SetNext(item Condition)
	SetPrev(item Condition)
	Column() ConditionColumn
	Operator() ConditionOperator
}

type ConditionColumn interface {
	Alias() string
	Column() string
	Original() string
}

type ConditionOperator interface {
	Original() string
	ConditionType() string
}

type ConditionValue interface {
	Original() string
	Value() string
}

type condition struct {
	value        ConditionValue
	column       ConditionColumn
	operator     ConditionOperator
	compoundHead Condition
	next         Condition
	prev         Condition
}

type conditionColumn struct {
	alias    string
	column   string
	original string
}

func (cc conditionColumn) Alias() string {
	return cc.alias
}

func (cc conditionColumn) Column() string {
	return cc.column
}

func (cc conditionColumn) Original() string {
	return cc.original
}

type conditionOperator struct {
	original      string
	conditionType string
}

func (co conditionOperator) Original() string {
	return co.original
}

func (co conditionOperator) ConditionType() string {
	return co.conditionType
}

type conditionValue struct {
	original string
	value    string
}

func (cv conditionValue) Original() string {
	return cv.original
}

func (cv conditionValue) Value() string {
	return cv.value
}

func (i *condition) Value() ConditionValue {
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

func (i *condition) Column() ConditionColumn {
	return i.column
}

func (i *condition) Operator() ConditionOperator {
	return i.operator
}

func NewCondition(column ConditionColumn, operator ConditionOperator, value ConditionValue) Condition {
	return &condition{
		value:    value,
		column:   column,
		operator: operator,
		next:     nil,
		prev:     nil,
	}
}

func NewConditionColumn(alias, column, original string) ConditionColumn {
	return conditionColumn{
		alias:    alias,
		column:   column,
		original: original,
	}
}

func NewConditionOperator(t, original string) ConditionOperator {
	return conditionOperator{
		original:      original,
		conditionType: t,
	}
}

func NewConditionValue(value, original string) ConditionValue {
	return conditionValue{
		original: original,
		value:    value,
	}
}
