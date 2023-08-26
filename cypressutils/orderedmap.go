package cypressutils

import (
	"container/list"
)

type Pair struct {
	Key   interface{}
	Value interface{}

	element *list.Element
}

type OrderedMap struct {
	pairs map[interface{}]*Pair
	list  *list.List
}

// NewOrderedMap creates a new OrderedMap.
func newOrderedMap(size ...int) *OrderedMap {
	if size != nil {
		return &OrderedMap{
			pairs: make(map[interface{}]*Pair, size[0]),
			list:  list.New(),
		}
	} else {
		return &OrderedMap{
			pairs: make(map[interface{}]*Pair),
			list:  list.New(),
		}
	}
}

// Get looks for the given key, and returns the value associated with it,
// or nil if not found. The boolean it returns says whether the key is present in the map.
func (om *OrderedMap) get(key interface{}) (interface{}, bool) {
	if pair, present := om.pairs[key]; present {
		return pair.Value, present
	}
	return nil, false
}

// getPair looks for the given key, and returns the pair associated with it,
// or nil if not found. The Pair struct can then be used to iterate over the ordered map
// from that point, either forward or backward.
func (om *OrderedMap) getPair(key interface{}) *Pair {
	return om.pairs[key]
}

// Set sets the key-value pair, and returns what `Get` would have returned
// on that key prior to the call to `Set`.
func (om *OrderedMap) set(key interface{}, value interface{}) (interface{}, bool) {
	if pair, present := om.pairs[key]; present {
		oldValue := pair.Value
		pair.Value = value
		return oldValue, true
	}

	pair := &Pair{
		Key:   key,
		Value: value,
	}
	pair.element = om.list.PushBack(pair)
	om.pairs[key] = pair

	return nil, false
}

// delete removes the key-value pair, and returns what `Get` would have returned
// on that key prior to the call to `delete`.
func (om *OrderedMap) delete(key interface{}) (interface{}, bool) {
	if pair, present := om.pairs[key]; present {
		om.list.Remove(pair.element)
		delete(om.pairs, key)
		return pair.Value, true
	}

	return nil, false
}

// len returns the length of the ordered map.
func (om *OrderedMap) len() int {
	return len(om.pairs)
}

// Oldest returns a pointer to the Oldest pair. It's meant to be used to iterate on the ordered map's
// pairs from the Oldest to the Newest, e.g.:
// for pair := orderedMap.Oldest(); pair != nil; pair = pair.Next() { fmt.Printf("%v => %v\n", pair.Key, pair.Value) }
func (om *OrderedMap) Oldest() *Pair {
	return listElementToPair(om.list.Front())
}

// Newest returns a pointer to the Newest pair. It's meant to be used to iterate on the ordered map's
// pairs from the Newest to the Oldest, e.g.:
// for pair := orderedMap.Oldest(); pair != nil; pair = pair.Next() { fmt.Printf("%v => %v\n", pair.Key, pair.Value) }
func (om *OrderedMap) Newest() *Pair {
	return listElementToPair(om.list.Back())
}

// Next returns a pointer to the Next pair.
func (p *Pair) Next() *Pair {
	return listElementToPair(p.element.Next())
}

// Previous returns a pointer to the previous pair.
func (p *Pair) Prev() *Pair {
	return listElementToPair(p.element.Prev())
}

func listElementToPair(element *list.Element) *Pair {
	if element == nil {
		return nil
	}
	return element.Value.(*Pair)
}
