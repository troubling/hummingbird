package hummingbird

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseRange(t *testing.T) {
	//Setting up individual test data
	tests := []struct {
		rangeHeader string
		exResBegin  int64
		exResEnd    int64
		exError     string
	}{
		{" ", 0, 0, ""},
		{"bytes=", 0, 0, "invalid range format"},
		{"bytes=-", 0, 0, "invalid range format"},
		{"bytes=-cv", 0, 0, "invalid end with no begin"},
		{"bytes=cv-", 0, 0, "invalid begin with no end"},
		{"bytes=-0", 0, 0, "zero end with no begin"},
		{"bytes=-12346", 0, 12345, ""},
		{"bytes=-12344", 1, 12345, ""},
		{"bytes=12344-", 12344, 12345, ""},
		{"bytes=12345-cv", 0, 0, "invalid end"},
		{"bytes=cv-12345", 0, 0, "invalid begin"},
		{"bytes=12346-123457", 0, 0, "Begin bigger than file"},
		{"bytes=12342-12343", 12342, 12344, ""},
		{"bytes=12342-12344", 12342, 12345, ""},
	}

	//Run tests with data from above
	for _, test := range tests {
		result, err := ParseRange(test.rangeHeader, 12345)
		if test.rangeHeader == " " {
			assert.Nil(t, result)
			assert.Nil(t, err)
			continue
		}
		if test.exError == "" {
			httpResult := httpRange{test.exResBegin, test.exResEnd}
			assert.Nil(t, err)
			assert.Contains(t, result, httpResult)
		} else {
			assert.Equal(t, err.Error(), test.exError)
		}
	}
}

func TestParseRange_BeginAfterEnd(t *testing.T) {
	result, err := ParseRange("bytes=12346-12", 12345)
	assert.Nil(t, err)
	assert.Empty(t, result)
}

func TestParseRange_NoEnd_BeginLargerThanFilesize(t *testing.T) {
	result, err := ParseRange("bytes=12346-", 12345)
	assert.Nil(t, err)
	assert.Empty(t, result)
}

func TestParseDate(t *testing.T) {
	//Setup tests with individual data
	tests := []string{
		"Mon, 02 Jan 2006 15:04:05 MST",
		"Mon, 02 Jan 2006 15:04:05 -0700",
		"Mon Jan 02 15:04:05 2006",
		"Monday, 02-Jan-06 15:04:05 MST",
		"1136214245",
		"1136214245.1234",
		"2006-01-02 15:04:05",
	}

	//Run Tests from above
	for _, timestamp := range tests {
		timeResult, err := ParseDate(timestamp)
		if err == nil {
			assert.Equal(t, timeResult.Day(), 02)
			assert.Equal(t, timeResult.Month(), 01)
			assert.Equal(t, timeResult.Year(), 2006)
			assert.Equal(t, timeResult.Hour(), 15)
			assert.Equal(t, timeResult.Minute(), 04)
			assert.Equal(t, timeResult.Second(), 05)
		} else {
			assert.Equal(t, err.Error(), "invalid time")
		}
	}

}

func TestStandardizeTimestamp(t *testing.T) {
	//Setup tests with individual data
	tests := []struct {
		timestamp      string
		expectedResult string
	}{
		{"12345.12345", "0000012345.12345"},
		{"12345.1234", "0000012345.12340"},
		{"12345.1234_123455", "0000012345.12340_0000000000123455"},
		{"12345.12343_12345a", "0000012345.12343_000000000012345a"},
	}

	//Run Tests from above
	for _, test := range tests {
		result, _ := StandardizeTimestamp(test.timestamp)
		assert.Equal(t, test.expectedResult, result)
	}

}

func TestStandardizeTimestamp_invalidTimestamp(t *testing.T) {
	//Setup test data
	tests := []struct {
		timestamp string
		errorMsg  string
	}{
		{"invalidTimestamp", "Could not parse float from 'invalidTimestamp'."},
		{"1234.1234_invalidOffset", "Could not parse int from 'invalidOffset'."},
	}
	for _, test := range tests {
		_, err := StandardizeTimestamp(test.timestamp)
		assert.Equal(t, err.Error(), test.errorMsg)
	}
}

func TestGetEpochFromTimestamp(t *testing.T) {
	//Setup tests with individual data
	tests := []struct {
		timestamp      string
		expectedResult string
	}{
		{"12345.12345", "0000012345.12345"},
		{"12345.1234", "0000012345.12340"},
		{"12345.1234_123455", "0000012345.12340"},
		{"12345.12343_12345a", "0000012345.12343"},
	}

	//Run Tests from above
	for _, test := range tests {
		result, _ := GetEpochFromTimestamp(test.timestamp)
		assert.Equal(t, test.expectedResult, result)
	}
}

func TestGetEpochFromTimestamp_invalidTimestamp(t *testing.T) {
	_, err := GetEpochFromTimestamp("invalidTimestamp")
	assert.Equal(t, err.Error(), "Could not parse float from 'invalidTimestamp'.")
}

func TestParseTimestamp(t *testing.T) {
	tests := []string{
		"2006-01-02 15:04:05",
		"Mon, 02 Jan 2006 15:04:05 MST",
	}

	for _, timestamp := range tests {
		timeResult, err := FormatTimestamp(timestamp)
		if err != nil {
			assert.Equal(t, err.Error(), "invalid time")
			assert.Empty(t, timeResult)
		} else {
			assert.Equal(t, "2006-01-02T15:04:05", timeResult)
		}
	}
}

func TestLooksTrue(t *testing.T) {
	tests := []string{
		"true ",
		"true",
		"t",
		"yes",
		"y",
		"1",
		"on",
	}

	for _, test := range tests {
		isTrue := LooksTrue(test)
		assert.True(t, isTrue)
	}
}

func TestValidTimestamp(t *testing.T) {
	assert.True(t, ValidTimestamp("12345.12345"))
	assert.False(t, ValidTimestamp("12345"))
	assert.False(t, ValidTimestamp("your.face"))
}

func TestUrlencode(t *testing.T) {
	assert.True(t, Urlencode("HELLOTHERE") == "HELLOTHERE")
	assert.True(t, Urlencode("HELLO THERE, YOU TWO//\x00\xFF") == "HELLO%20THERE%2C%20YOU%20TWO//%00%FF")
	assert.True(t, Urlencode("鐋댋") == "%E9%90%8B%EB%8C%8B")
}
