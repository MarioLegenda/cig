package syntaxStructure

type Constraint[T any] interface {
	Value() T
}

type StructureConstraints interface {
	Limit() Constraint[int64]
	Offset() Constraint[int64]
}

type limit[T comparable] struct {
	value T
}

type offset[T comparable] struct {
	value T
}

type constraints struct {
	limit  Constraint[int64]
	offset Constraint[int64]
}

func (c constraints) Limit() Constraint[int64] {
	return c.limit
}

func (c constraints) Offset() Constraint[int64] {
	return c.offset
}

func (c limit[T]) Value() T {
	return c.value
}

func (c offset[T]) Value() T {
	return c.value
}

func NewLimit(value int64) Constraint[int64] {
	return limit[int64]{value: value}
}

func NewOffset(value int64) Constraint[int64] {
	return offset[int64]{value: value}
}

func NewConstraints(limit Constraint[int64], offset Constraint[int64]) StructureConstraints {
	return constraints{
		limit:  limit,
		offset: offset,
	}
}
