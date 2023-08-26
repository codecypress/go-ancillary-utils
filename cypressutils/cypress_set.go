package cypressutils

var exists = struct{}{}

type Set struct {
	m map[interface{}]struct{}
}

func NewSet() *Set {
	s := &Set{}
	s.m = make(map[interface{}]struct{})
	return s
}

func (s *Set) Add(value interface{}) {
	s.m[value] = exists
}

func (s *Set) Remove(value interface{}) {
	delete(s.m, value)
}

func (s *Set) Contains(value interface{}) bool {
	_, c := s.m[value]
	return c
}

func (s *Set) GetValues() []interface{} {
	keys := make([]interface{}, len(s.m))

	i := 0
	for k := range s.m {
		keys[i] = k
		i++
	}
	return keys
}
