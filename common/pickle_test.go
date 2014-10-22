package hummingbird

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

func PickleRoundTrip(t *testing.T, v interface{}) (interface{}) {
  	str := PickleDumps(v)
	ret, err := PickleLoads(str)
	if err != nil {
	  	t.Fatal("Error parsing pickle: " + err.Error())
	}
	return ret
}

func TestRoundTrip1(t *testing.T) {
  	if dataVal, ok := PickleRoundTrip(t, 2).(int64); ok {
	  	if dataVal != 2 {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func TestRoundTrip2(t *testing.T) {
  	if dataVal, ok := PickleRoundTrip(t, "hi").(string); ok {
	  	if dataVal != "hi" {
			t.Fatal("Return data not correct.")
		}
	} else {
		t.Fatal("Invalid return type.")
	}
}

func TestRoundTrip3(t *testing.T) {
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
