package common

import (
	"testing"
)

func TestUnpicklingVersion1Map(t *testing.T) {
	data, err := PickleLoads([]byte("(dp1\nS'hi'\np2\nS'there'\np3\ns."))
	if err != nil {
		t.Fatal("Error parsing pickle: " + err.Error())
	}
	if dataVal, ok := data.(map[interface{}]interface{}); ok {
		if dataVal["hi"] != "there" {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func TestUnpicklingVersion2Map(t *testing.T) {
	data, err := PickleLoads([]byte("\x80\x02}q\x01U\x02hiq\x02U\x05thereq\x03s."))
	if err != nil {
		t.Fatal("Error parsing pickle: " + err.Error())
	}
	if dataVal, ok := data.(map[interface{}]interface{}); ok {
		if dataVal["hi"] != "there" {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func PickleRoundTrip(t *testing.T, v interface{}) interface{} {
	ret, err := PickleLoads(PickleDumps(v))
	if err != nil {
		t.Fatal("Error parsing pickle: " + err.Error())
	}
	return ret
}

func TestPickleInt(t *testing.T) {
	testCases := []int64{2, 1 << 10, 1 << 33, -1, 0 - (1 << 33)}
	for _, testCase := range testCases {
		if dataVal, ok := PickleRoundTrip(t, testCase).(int64); ok {
			if dataVal != int64(testCase) {
				t.Fatal("Return data not correct.")
			}
		} else {
			t.Fatal("Invalid return type.")
		}
	}
}

func TestPickleIntTypes(t *testing.T) {
	testCases := []interface{}{uint(8), int16(8), uint16(8), int32(8), uint32(8), int64(8), uint64(8)}
	for _, testCase := range testCases {
		if dataVal, ok := PickleRoundTrip(t, testCase).(int64); ok {
			if dataVal != 8 {
				t.Fatal("Return data not correct.")
			}
		} else {
			t.Fatal("Invalid return type.")
		}
	}
}

func TestPickleFloat32(t *testing.T) {
	if dataVal, ok := PickleRoundTrip(t, float32(3.14159)).(float64); ok {
		if int64(dataVal*10000) != 31415 {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func TestPickleFloat64(t *testing.T) {
	if dataVal, ok := PickleRoundTrip(t, 3.14159).(float64); ok {
		if int64(dataVal*10000) != 31415 {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func TestPickleString(t *testing.T) {
	if dataVal, ok := PickleRoundTrip(t, "hi").(string); ok {
		if dataVal != "hi" {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func TestPickleLongString(t *testing.T) {
	longString := string(make([]byte, 1024))
	if dataVal, ok := PickleRoundTrip(t, longString).(string); ok {
		if dataVal != longString {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func TestPickleMapStringString(t *testing.T) {
	data := map[string]string{"1": "test1", "2": "test2"}
	if dataVal, ok := PickleRoundTrip(t, data).(map[interface{}]interface{}); ok {
		if dataVal["1"] != "test1" {
			t.Fatal("Return data not correct.")
		}
		if dataVal["2"] != "test2" {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func TestPickleMapStringInterface(t *testing.T) {
	data := map[string]interface{}{"1": "test1", "2": "test2"}
	if dataVal, ok := PickleRoundTrip(t, data).(map[interface{}]interface{}); ok {
		if dataVal["1"] != "test1" {
			t.Fatal("Return data not correct.")
		}
		if dataVal["2"] != "test2" {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func TestPickleSliceInterface(t *testing.T) {
	data := []interface{}{1, 2, 3}
	if dataVal, ok := PickleRoundTrip(t, data).([]interface{}); ok {
		if dataVal[0].(int64) != 1 {
			t.Fatal("Return data not correct.")
		}
		if dataVal[1].(int64) != 2 {
			t.Fatal("Return data not correct.")
		}
		if dataVal[2].(int64) != 3 {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func TestPickleNil(t *testing.T) {
	dataVal := PickleRoundTrip(t, nil)
	if dataVal != nil {
		t.Fatal("Return data not correct.")
	}
}

func TestPythonString(t *testing.T) { // TODO(redbo) needs way more tests.
	str, err := PythonString("\"hi \\\" there\"")
	if err != nil || str != "hi \" there" {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleBigPickle(t *testing.T) {
	// just to grow the stack beyond its default
	v, err := PickleLoads([]byte("\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88."))
	if err != nil || v.(bool) != true {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleBool(t *testing.T) {
	v, err := PickleLoads([]byte("\x88."))
	if err != nil || v.(bool) != true {
		t.Fatal("Return data not correct.")
	}
	v, err = PickleLoads([]byte("\x89."))
	if err != nil || v.(bool) != false {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleTuple1(t *testing.T) {
	v, err := PickleLoads([]byte("\x88\x85."))
	if err != nil || v.([]interface{})[0].(bool) != true {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleTuple2(t *testing.T) {
	v, err := PickleLoads([]byte("\x88\x89\x86."))
	tuple := v.([]interface{})
	if err != nil || len(tuple) != 2 || tuple[0].(bool) != true || tuple[1].(bool) != false {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleTuple3(t *testing.T) {
	v, err := PickleLoads([]byte("K\x00K\x01K\x02\x87."))
	tuple := v.([]interface{})
	if err != nil || len(tuple) != 3 || tuple[0].(int64) != 0 || tuple[1].(int64) != 1 || tuple[2].(int64) != 2 {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleDup(t *testing.T) {
	v, err := PickleLoads([]byte("K\x002\x86."))
	tuple := v.([]interface{})
	if err != nil || len(tuple) != 2 || tuple[0].(int64) != 0 || tuple[1].(int64) != 0 {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpicklePop(t *testing.T) {
	v, err := PickleLoads([]byte("K\x00K\x010."))
	if err != nil || v.(int64) != 0 {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleOldInt(t *testing.T) {
	v, err := PickleLoads([]byte("I12345\n."))
	if err != nil || v.(int64) != 12345 {
		t.Fatal("Return data not correct.")
	}

	v, err = PickleLoads([]byte("L12345\n."))
	if err != nil || v.(int64) != 12345 {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleOldFloat(t *testing.T) {
	v, err := PickleLoads([]byte("F3.14159\n."))
	if err != nil || int64(v.(float64)*10000) != 31415 {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleOldStupidMap(t *testing.T) {
	v, _ := PickleLoads([]byte("}(K\x00K\x01K\x02K\x03u."))
	m := v.(map[interface{}]interface{})
	if m[int64(0)].(int64) != 1 || m[int64(2)].(int64) != 3 {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleOldStupidList(t *testing.T) {
	v, _ := PickleLoads([]byte("]K\xffa."))
	l := v.([]interface{})
	if len(l) != 1 || l[0].(int64) != 255 {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpickleListAppends(t *testing.T) {
	v, _ := PickleLoads([]byte("](K\x01K\x02K\x03e."))
	l := v.([]interface{})
	if len(l) != 3 || l[0].(int64) != 1 || l[1].(int64) != 2 || l[2].(int64) != 3 {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpicklePopMark(t *testing.T) {
	v, _ := PickleLoads([]byte("K\xFF(K\x01K\x02K\x031."))
	if v.(int64) != 255 {
		t.Fatal("Return data not correct.")
	}
}

func TestGetPutMemo(t *testing.T) {
	v, _ := PickleLoads([]byte("K\xFFp5\nK\x00g5\n."))
	if v.(int64) != 255 {
		t.Fatal("Return data not correct.")
	}
}

func TestBinGetPutMemo(t *testing.T) {
	v, _ := PickleLoads([]byte("K\xFFq\x05K\x00h\x05."))
	if v.(int64) != 255 {
		t.Fatal("Return data not correct.")
	}
}

func TestLongbinGetPutMemo(t *testing.T) {
	v, _ := PickleLoads([]byte("K\xFFr1234K\x00j1234."))
	if v.(int64) != 255 {
		t.Fatal("Return data not correct.")
	}
}

func TestMixedGetPutMemo(t *testing.T) {
	v, _ := PickleLoads([]byte("K\xFFq\x05K\x00g5\n.")) // binary put non-binary get
	if v.(int64) != 255 {
		t.Fatal("Return data not correct.")
	}

	v, _ = PickleLoads([]byte("K\xFFp5\nK\x00h\x05.")) // non-binary put binary get
	if v.(int64) != 255 {
		t.Fatal("Return data not correct.")
	}

	v, _ = PickleLoads([]byte("K\xFFr\x05\x00\x00\x00K\x00g5\n.")) // longbin put non-binary get
	if v.(int64) != 255 {
		t.Fatal("Return data not correct.")
	}

	v, _ = PickleLoads([]byte("K\xFFp5\nK\x00j\x05\x00\x00\x00.")) // non-binary put longbin get
	if v.(int64) != 255 {
		t.Fatal("Return data not correct.")
	}
}

func TestShortPickles(t *testing.T) {
	// all invalid pickles, up coverage of error cases
	tests := []string{"", "S", "U", "U1", "T", "T1234", "I", "Iabc\n", "F", "Fabc\n", "K",
		"M", "J", "\x8a", "G", "p", "px\n", "g", "gx\n", "h", "j", "r", "q", "?", "SX\n", "\x8a\x01"}
	for _, test := range tests {
		if _, err := PickleLoads([]byte(test)); err == nil {
			t.Fatal("Pickle load didn't error.")
		}
	}
}
