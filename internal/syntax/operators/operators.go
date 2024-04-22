package operators

const EqualOperator = "="
const UnEqualOperator = "!="
const LessThanOperator = "<"
const GreaterThanOperator = ">"
const GreaterThanOrEqualOperator = ">="
const LessThanOrEqualOperator = "<="

const AndOperator = "and"
const OrOperator = "or"

var Operators = []string{
	EqualOperator,
	UnEqualOperator,
	LessThanOperator,
	LessThanOrEqualOperator,
	GreaterThanOperator,
	GreaterThanOrEqualOperator,
}

const LimitConstraint = "limit"
const OffsetConstraint = "offset"
const OrderByConstraint = "order by"

const Asc = "asc"
const Desc = "desc"

var Constraints = []string{
	LimitConstraint,
	OffsetConstraint,
	OrderByConstraint,
}
