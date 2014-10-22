package hummingbird

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var markster = "HI, I'M MARK!"
var mark = interface{}(&markster)

type Stack struct {
	stack []interface{}
	top   int
}

func (s *Stack) Push(item interface{}) {
	if len(s.stack) < s.top+1 {
		s.stack = append(s.stack, item)
	} else {
		s.stack[s.top] = item
	}
	s.top++
}

func (s *Stack) Pop() interface{} {
	s.top -= 1
	return s.stack[s.top]
}

func (s *Stack) Peek() interface{} {
	return s.stack[s.top-1]
}

func (s *Stack) SetMark() {
	s.Push(mark)
}

func (s *Stack) Mark() []interface{} {
	start := s.top
	for s.stack[s.top-1] != mark {
		s.top--
	}
	s.top--
	return s.stack[s.top+1 : start]
}

func NewStack(initialSize int) *Stack {
	return &Stack{make([]interface{}, initialSize), 0}
}

type MyBuffer struct {
	*bytes.Buffer
}

func (buf *MyBuffer) NextBytes(length int) ([]byte, error) {
	retval := buf.Next(length)
	if len(retval) != length {
		return nil, errors.New("Not enough bytes to read")
	}
	return retval, nil
}

func PythonString(src string) (string, error) {
	backslashQuote, _ := regexp.Compile("\\\\*('|\")")
	if src[0] == '\'' {
		src = src[1 : len(src)-1]
		src = backslashQuote.ReplaceAllStringFunc(src,
			func(thingy string) string {
				if thingy[len(thingy)-1] == '"' {
					if len(thingy)%2 == 1 {
						return "\\" + thingy
					}
					return thingy
				} else if len(thingy)%2 == 0 {
					return thingy[1:]
				}
				return thingy
			})
		src = "\"" + src + "\""
	}
	return strconv.Unquote(src)
}

func PickleLoads(data []byte) (interface{}, error) {
	memo := make(map[int]interface{})
	buf := &MyBuffer{bytes.NewBuffer(data)}
	stack := NewStack(8)
	for buf.Len() > 0 {
		op, err := buf.ReadByte()
		if err != nil {
			return nil, errors.New("Incomplete pickle (reading opcode): " + err.Error())
		}
		switch op {
		case '\x80': // PROTO
			buf.ReadByte()
		case '(': // MARK
			stack.SetMark()
		case '.': // STOP
			return stack.Pop(), nil
		case '0': // POP
			stack.Pop()
		case '1': // POP_MARK
			stack.Mark()
		case '2': // DUP
			stack.Push(stack.Peek())
		case '\x88': // NEWTRUE
			stack.Push(interface{}(true))
		case '\x89': // NEWFALSE
			stack.Push(interface{}(false))

		case 'S', 'V': // STRING, UNICODE
			val, err := buf.ReadString('\n')
			if err != nil {
				return nil, errors.New("Incomplete pickle (STRING): " + err.Error())
			}
			str, err := PythonString(strings.TrimRight(val, "\n"))
			if err != nil {
				return nil, errors.New("Unable to interpret Python string (STRING): " + err.Error())
			}
			stack.Push(str)
		case 'U': //SHORT_BINSTRING
			length, err := buf.ReadByte()
			if err != nil {
				return nil, errors.New("Incomplete pickle (SHORT_BINSTRING): " + err.Error())
			}
			str, err := buf.NextBytes(int(length))
			if err != nil {
				return nil, errors.New("Incomplete pickle (SHORT_BINSTRING): " + err.Error())
			}
			stack.Push(string(str))
		case 'T', 'X': // BINSTRING, BINUNICODE
			lenb, err := buf.NextBytes(4)
			if err != nil {
				return nil, errors.New("Incomplete pickle (BINSTRING): " + err.Error())
			}
			str, err := buf.NextBytes(int(binary.LittleEndian.Uint32(lenb)))
			if err != nil {
				return nil, errors.New("Incomplete pickle (BINSTRING): " + err.Error())
			}
			stack.Push(string(str))

		case 's': // SETITEM
			val := stack.Pop()
			key := stack.Pop()
			stack.Peek().(map[interface{}]interface{})[key] = val
		case 'u': // SETITEMS
			vals := stack.Mark()
			dict := stack.Pop().(map[interface{}]interface{})
			for j := 0; j < len(vals); j += 2 {
				dict[vals[j]] = vals[j+1]
			}
			stack.Push(dict)

		case '}': // EMPTY_DICT
			stack.Push(make(map[interface{}]interface{}))
		case 'd': // DICT
			dict := make(map[interface{}]interface{})
			vals := stack.Mark()
			for j := 0; j < len(vals); j += 2 {
				dict[vals[j]] = vals[j+1]
			}
			stack.Push(dict)
		case ']', ')': // EMPTY_LIST, EMPTY_TUPLE
			stack.Push(make([]interface{}, 0))
		case 'l', 't': // LIST, TUPLE
			stack.Push(stack.Mark())
		case 'a': // APPEND
			value := stack.Pop()
			list := stack.Pop()
			stack.Push(append(list.([]interface{}), value))
		case 'N': // NONE
			stack.Push(nil)
		case '\x85': // TUPLE1
			stack.Push([]interface{}{stack.Pop()})
		case '\x86': // TUPLE2
			stack.Push([]interface{}{stack.Pop(), stack.Pop()})
		case '\x87': // TUPLE3
			stack.Push([]interface{}{stack.Pop(), stack.Pop(), stack.Pop()})
		case 'e': // APPENDS
			items := stack.Mark()
			stack.Push(append(stack.Pop().([]interface{}), items...))

		case 'I', 'L': // INT, LONG
			line, err := buf.ReadString('\n')
			if err != nil {
				return nil, errors.New("Incomplete pickle (INT): " + err.Error())
			}
			val, err := strconv.ParseInt(strings.TrimRight(line, "\n"), 10, 64)
			if err != nil {
				return nil, errors.New("Invalid pickle (INT): " + err.Error())
			}
			stack.Push(val)
		case 'F': // FLOAT
			line, err := buf.ReadString('\n')
			if err != nil {
				return nil, errors.New("Incomplete pickle (FLOAT): " + err.Error())
			}
			val, err := strconv.ParseFloat(strings.TrimRight(line, "\n"), 64)
			if err != nil {
				return nil, errors.New("Invalid pickle (FLOAT): " + err.Error())
			}
			stack.Push(val)
		case 'K': // BININT1
			val, err := buf.ReadByte()
			if err != nil {
				return nil, errors.New("Incomplete pickle (BININT1): " + err.Error())
			}
			stack.Push(int64(val))
		case 'M': // BININT2
			valb, err := buf.NextBytes(2)
			if err != nil {
				return nil, errors.New("Incomplete pickle (BININT2): " + err.Error())
			}
			stack.Push(int64(binary.LittleEndian.Uint16(valb)))
		case 'J': // BININT4
			valb, err := buf.NextBytes(4)
			if err != nil {
				return nil, errors.New("Incomplete pickle (BININT4): " + err.Error())
			}
			stack.Push(int64(binary.LittleEndian.Uint32(valb)))
		case '\x8a': // LONG1
			length, err := buf.ReadByte()
			if err != nil {
				return nil, errors.New("Incomplete pickle (LONG1): " + err.Error())
			}
			val := int64(0)
			if length > 0 {
				valb, err := buf.NextBytes(int(length))
				if err != nil {
					return nil, errors.New("Incomplete pickle (LONG1): " + err.Error())
				}
				for i, d := range valb {
					val |= (int64(d) << uint64(i*8))
				}
				buf.UnreadByte()
				lastByte, err := buf.ReadByte()
				if err != nil {
					return nil, errors.New("Incomplete pickle (LONG1): " + err.Error())
				}
				if lastByte >= '\x80' {
					val -= int64(1) << uint64(length*8)
				}
			}
			stack.Push(val)
		case 'G': // BINFLOAT
			valb, err := buf.NextBytes(8)
			if err != nil {
				return nil, errors.New("Incomplete pickle (BINFLOAT): " + err.Error())
			}
			stack.Push(math.Float64frombits(binary.LittleEndian.Uint64(valb)))

		case 'p': // PUT
			line, err := buf.ReadString('\n')
			if err != nil {
				return nil, errors.New("Incomplete pickle (PUT): " + err.Error())
			}
			id, err := strconv.ParseInt(strings.TrimRight(line, "\n"), 10, 64)
			if err != nil {
				return nil, errors.New("Invalid pickle (PUT): " + err.Error())
			}
			memo[int(id)] = stack.Peek()
		case 'g': // GET
			line, err := buf.ReadString('\n')
			if err != nil {
				return nil, errors.New("Incomplete pickle (GET): " + err.Error())
			}
			id, err := strconv.ParseInt(strings.TrimRight(line, "\n"), 10, 64)
			if err != nil {
				return nil, errors.New("Invalid pickle (GET): " + err.Error())
			}
			stack.Push(memo[int(id)])
		case 'q': // BINPUT
			val, err := buf.ReadByte()
			if err != nil {
				return nil, errors.New("Incomplete pickle (BINPUT): " + err.Error())
			}
			memo[int(val)] = stack.Peek()
		case 'h': // BINGET
			val, err := buf.ReadByte()
			if err != nil {
				return nil, errors.New("Incomplete pickle (BINGET): " + err.Error())
			}
			stack.Push(memo[int(val)])
		case 'j': // LONG_BINGET
			valb, err := buf.NextBytes(4)
			if err != nil {
				return nil, errors.New("Incomplete pickle (LONG_BINGET): " + err.Error())
			}
			id := int(binary.LittleEndian.Uint32(valb))
			stack.Push(memo[id])
		case 'r': // LONG_BINPUT = 'r' //   "     "    "   "   " ;   "    " 4-byte arg
			valb, err := buf.NextBytes(4)
			if err != nil {
				return nil, errors.New("Incomplete pickle (LONG_BINPUT): " + err.Error())
			}
			id := int(binary.LittleEndian.Uint32(valb))
			memo[id] = stack.Peek()
		default:
			return nil, errors.New(fmt.Sprintf("Unknown pickle opcode: %c (%x)\n", op, op))
		}
	}
	return nil, errors.New("Incomplete pickle: fell out of loop")
}
