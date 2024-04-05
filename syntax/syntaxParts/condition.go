package syntaxParts

type item struct {
	value string
	next  Item
	prev  Item
}

type Item interface {
	Value() string
	Next() Item
	Prev() Item
}

func (i *item) Value() string {
	return i.value
}

func (i *item) Next() Item {
	return i.next
}

func (i *item) Prev() Item {
	return i.prev
}

func NewItem(value string) Item {
	return &item{
		value: value,
		next:  nil,
		prev:  nil,
	}
}
