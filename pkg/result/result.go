package result

type result[T any] struct {
	errs   []error
	result T
}

type Result[T any] interface {
	Errors() []error
	HasErrors() bool
	Result() T
}

func (r result[T]) Errors() []error {
	return r.errs
}

func (r result[T]) HasErrors() bool {
	return len(r.errs) != 0
}

func (r result[T]) Result() T {
	return r.result
}

func NewResult[T any](res T, errs []error) Result[T] {
	if len(errs) == 0 || errs == nil {
		return result[T]{errs: nil, result: res}
	}

	return result[T]{errs: errs, result: res}
}
