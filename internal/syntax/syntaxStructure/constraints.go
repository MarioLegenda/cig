package syntaxStructure

type Constraint[T any] interface {
	Value() T
}

type OrderBy interface {
	Columns() []OrderByColumn
	Direction() string
}

type OrderByColumn interface {
	Column() string
	Alias() string
}

type StructureConstraints interface {
	Limit() Constraint[int64]
	Offset() Constraint[int64]
	OrderBy() OrderBy
}

type limit[T comparable] struct {
	value T
}

type offset[T comparable] struct {
	value T
}

type orderByColumn struct {
	column string
	alias  string
}

type orderBy struct {
	columns   []OrderByColumn
	direction string
}

type constraints struct {
	limit   Constraint[int64]
	offset  Constraint[int64]
	orderBy OrderBy
}

func (obc orderByColumn) Column() string {
	return obc.column
}

func (obc orderByColumn) Alias() string {
	return obc.alias
}

func (c constraints) Limit() Constraint[int64] {
	return c.limit
}

func (c constraints) Offset() Constraint[int64] {
	return c.offset
}

func (c constraints) OrderBy() OrderBy {
	return c.orderBy
}

func (c limit[T]) Value() T {
	return c.value
}

func (c offset[T]) Value() T {
	return c.value
}

func (c orderBy) Columns() []OrderByColumn {
	return c.columns
}

func (c orderBy) Direction() string {
	return c.direction
}

func newOrderByColumn(c string, alias string) OrderByColumn {
	return orderByColumn{
		column: c,
		alias:  alias,
	}
}

func NewOrderBy(columns map[string]string, direction string) OrderBy {
	obs := make([]OrderByColumn, 0)
	for c, alias := range columns {
		obs = append(obs, newOrderByColumn(c, alias))
	}

	return orderBy{
		columns:   obs,
		direction: direction,
	}
}

func NewLimit(value int64) Constraint[int64] {
	return limit[int64]{value: value}
}

func NewOffset(value int64) Constraint[int64] {
	return offset[int64]{value: value}
}

func NewConstraints(limit Constraint[int64], offset Constraint[int64], ob OrderBy) StructureConstraints {
	return constraints{
		limit:   limit,
		offset:  offset,
		orderBy: ob,
	}
}
