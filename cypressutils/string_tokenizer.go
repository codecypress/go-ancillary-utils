package cypressutils

import "strings"

const const_MIN_SUPPLEMENTARY_CODE_POINT = 65536
const const_MIN_HIGH_SURROGATE = 55296
const const_MAX_LOW_SURROGATE = 57343

type StringTokenizer struct {
	currentPosition, newPosition, maxPosition, maxDelimCodePoint int
	str, delimiters                                              string
	retDelims, delimsChanged, hasSurrogates                      bool
	delimiterCodePoints                                          []int
}

func (tokenizer *StringTokenizer) setMaxDelimCodePoint() {
	if tokenizer.delimiters == "" {
		tokenizer.maxDelimCodePoint = 0
		return
	}

	m := 0
	var c int
	count := 0
	for i := 0; i < len(tokenizer.delimiters); i += charCount(c) {
		c = int(tokenizer.delimiters[i])
		if c >= const_MIN_HIGH_SURROGATE && c <= const_MAX_LOW_SURROGATE {
			c = int(tokenizer.delimiters[i])
			tokenizer.hasSurrogates = true
		}
		if m < c {
			m = c
		}

		count++
	}

	tokenizer.maxDelimCodePoint = m

	if tokenizer.hasSurrogates {
		tokenizer.delimiterCodePoints = make([]int, count)
		for i, j := 0, 0; i < count; i, j = i+1, j+charCount(c) {
			c = int(tokenizer.delimiters[j])
			tokenizer.delimiterCodePoints[i] = c
		}
	}
}

func NewStringTokenizer(str, delim string, returnDelims bool) *StringTokenizer {
	tokenizer := &StringTokenizer{
		currentPosition: 0,
		newPosition:     -1,
		delimsChanged:   false,
		str:             str,
		maxPosition:     len(str),
		delimiters:      delim,
		retDelims:       returnDelims,
	}
	tokenizer.setMaxDelimCodePoint()
	return tokenizer
}

func (tokenizer *StringTokenizer) skipDelimiters(startPos int) int {

	position := startPos
	for !tokenizer.retDelims && position < tokenizer.maxPosition {
		if !tokenizer.hasSurrogates {
			c := int(tokenizer.str[position])
			if (c > tokenizer.maxDelimCodePoint) ||
				(strings.Index(tokenizer.delimiters, string(tokenizer.str[position])) < 0) {
				break
			}
			position++
		} else {
			c := int(tokenizer.str[position])
			if (c > tokenizer.maxDelimCodePoint) || !tokenizer.isDelimiter(c) {
				break
			}
			position += charCount(c)
		}
	}
	return position
}

func (tokenizer *StringTokenizer) scanToken(startPos int) int {
	position := startPos
	for position < tokenizer.maxPosition {
		if !tokenizer.hasSurrogates {
			c := int(tokenizer.str[position])
			if (c <= tokenizer.maxDelimCodePoint) &&
				(strings.Index(tokenizer.delimiters, string(tokenizer.str[position])) >= 0) {
				break
			}
			position++
		} else {
			c := int(tokenizer.str[position])
			if (c <= tokenizer.maxDelimCodePoint) && tokenizer.isDelimiter(c) {
				break
			}
			position += charCount(c)
		}
	}
	if tokenizer.retDelims && (startPos == position) {
		if !tokenizer.hasSurrogates {
			c := int(tokenizer.str[position])
			if (c <= tokenizer.maxDelimCodePoint) &&
				(strings.Index(tokenizer.delimiters, string(tokenizer.str[position])) >= 0) {
				position++
			}
		} else {
			c := int(tokenizer.str[position])
			if (c <= tokenizer.maxDelimCodePoint) && tokenizer.isDelimiter(c) {
				position += charCount(c)
			}
		}
	}
	return position
}

func (tokenizer *StringTokenizer) isDelimiter(codePoint int) bool {
	for i := 0; i < len(tokenizer.delimiterCodePoints); i++ {
		if tokenizer.delimiterCodePoints[i] == codePoint {
			return true
		}
	}
	return false
}

func (tokenizer *StringTokenizer) HasMoreTokens() bool {
	tokenizer.newPosition = tokenizer.skipDelimiters(tokenizer.currentPosition)
	return tokenizer.newPosition < tokenizer.maxPosition
}

func (tokenizer *StringTokenizer) NextToken(delim ...string) string {
	if delim != nil {
		tokenizer.delimiters = delim[0]
		tokenizer.delimsChanged = true
		tokenizer.setMaxDelimCodePoint()
		return tokenizer.NextToken()
	}

	if tokenizer.newPosition >= 0 && !tokenizer.delimsChanged {
		tokenizer.currentPosition = tokenizer.newPosition
	} else {
		tokenizer.currentPosition = tokenizer.skipDelimiters(tokenizer.currentPosition)
	}

	tokenizer.delimsChanged = false
	tokenizer.newPosition = -1

	if tokenizer.currentPosition >= tokenizer.maxPosition {
		panic("No Such Element")
	}

	start := tokenizer.currentPosition
	tokenizer.currentPosition = tokenizer.scanToken(tokenizer.currentPosition)
	return tokenizer.str[start:tokenizer.currentPosition]
}

func (tokenizer *StringTokenizer) CountTokens() int {
	count := 0
	currpos := tokenizer.currentPosition
	for currpos < tokenizer.maxPosition {
		currpos = tokenizer.skipDelimiters(currpos)
		if currpos >= tokenizer.maxPosition {
			break
		}
		currpos = tokenizer.scanToken(currpos)
		count++
	}
	return count
}

func charCount(c int) int {
	if c >= const_MIN_SUPPLEMENTARY_CODE_POINT {
		return 2
	}
	return 1
}
