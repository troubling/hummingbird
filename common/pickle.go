package hummingbird

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

func pickleint(val int64, buf *bytes.Buffer) {
	if int64(uint8(val)) == val {
		buf.WriteByte('K')
		buf.WriteByte(byte(val))
	} else if int64(uint16(val)) == val {
		buf.WriteByte('M')
		ret := make([]byte, 2)
		binary.LittleEndian.PutUint16(ret, uint16(val))
		buf.Write(ret)
	} else if int64(int32(val)) == val {
		buf.WriteByte('J')
		ret := make([]byte, 4)
		binary.LittleEndian.PutUint32(ret, uint32(val))
		buf.Write(ret)
	} else {
		buf.WriteByte('\x8a')
		buf.WriteByte(8)
		ret := make([]byte, 8)
		binary.LittleEndian.PutUint64(ret, uint64(val))
		buf.Write(ret)
	}
}

func picklefloat(val float64, buf *bytes.Buffer) {
	buf.WriteByte('G')
	bits := math.Float64bits(val)
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, bits)
	buf.Write(ret)
}

func pickleobj(o interface{}, buf *bytes.Buffer) {
	switch o := o.(type) {
	case int:
		pickleint(int64(o), buf)
	case uint:
		pickleint(int64(o), buf)
	case int16:
		pickleint(int64(o), buf)
	case uint16:
		pickleint(int64(o), buf)
	case int32:
		pickleint(int64(o), buf)
	case uint32:
		pickleint(int64(o), buf)
	case int64:
		pickleint(int64(o), buf)
	case uint64:
		pickleint(int64(o), buf)

	case float32:
		picklefloat(float64(o), buf)
	case float64:
		picklefloat(float64(o), buf)

	case string:
		length := len(o)
		if length < 256 {
			buf.Write([]byte{'U', byte(length)})
			buf.WriteString(o)
		} else {
			buf.WriteByte('T')
			lens := make([]byte, 4)
			binary.LittleEndian.PutUint32(lens, uint32(length))
			buf.Write(lens)
			buf.WriteString(o)
		}
	case map[interface{}]interface{}:
		buf.WriteByte('(')
		for k, v := range o {
			pickleobj(k, buf)
			pickleobj(v, buf)
		}
		buf.WriteByte('d')
	case map[string]string:
		buf.WriteByte('(')
		for k, v := range o {
			pickleobj(k, buf)
			pickleobj(v, buf)
		}
		buf.WriteByte('d')
	case map[string]interface{}:
		buf.WriteByte('(')
		for k, v := range o {
			pickleobj(k, buf)
			pickleobj(v, buf)
		}
		buf.WriteByte('d')
	case []interface{}:
		buf.WriteByte('(')
		for _, item := range o {
			pickleobj(item, buf)
		}
		buf.WriteByte('l')
	default:
		panic(fmt.Sprintf("Unknown object type in pickle:", o))
	}
}

func PickleDumps(o interface{}) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte('\x80')
	buf.WriteByte(2)
	pickleobj(o, buf)
	buf.WriteByte('.')
	return buf.Bytes()
}
