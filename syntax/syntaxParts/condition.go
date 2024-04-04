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
	SetNext(item Item)
	SetPrev(item Item)
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

func (i *item) SetNext(item Item) {
	i.next = item
}

func (i *item) SetPrev(item Item) {
	i.prev = item
}

func NewItem(value string) Item {
	return &item{
		value: value,
		next:  nil,
		prev:  nil,
	}
}
