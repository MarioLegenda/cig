package pkg

type result[T any] struct {
	error  error
	result T
}

type Result[T any] interface {
	Error() error
	Result() T
}

func (r result[T]) Error() error {
	return r.error
}

func (r result[T]) Result() T {
	return r.result
}

func NewResult[T any](res T, errs error) Result[T] {
	return result[T]{error: nil, result: res}

}
