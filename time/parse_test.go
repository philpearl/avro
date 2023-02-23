package time

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	tests := []string{
		"2006-01-02T13:37:42Z",
		"2006-01-02T13:37:42,326Z",
		"2006-01-02T13:37:42.0Z",
		"2006-01-02T13:37:42.0000Z",
		"2006-01-02T13:37:42,326876Z",
		"2006-01-02T13:37:42,326876123Z",
		"2006-01-02T13:37:42.326876123Z",
		"2006-01-02T13:37:42,326+08:00",
		"2006-01-02T13:37:42.326-08:00",
		"2006-01-02T13:37:42.326-08:21",
		"2006-01-02T13:37:42.326+08:21",
		"2021-09-30T08:28:33.137578Z",
	}

	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			exp, err := time.Parse(time.RFC3339, test)
			if err != nil {
				t.Fatal(err)
			}

			actual, err := parseTime(test)
			if err != nil {
				t.Fatal(err)
			}

			if !exp.Equal(actual) {
				t.Fatalf("parsed time incorrect. Got %s for %s", actual, test)
			}
		})
	}
}

func TestParseDate(t *testing.T) {
	tests := []string{
		"2021-09-30",
		"1970-01-01",
	}

	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			exp, err := time.Parse(time.DateOnly, test)
			if err != nil {
				t.Fatal(err)
			}

			actual, err := parseTime(test)
			if err != nil {
				t.Fatal(err)
			}

			if !exp.Equal(actual) {
				t.Fatalf("parsed time incorrect. Got %s for %s", actual, test)
			}
		})
	}
}

func TestParseFails(t *testing.T) {
	tests := []string{
		"",
		"2006",
		"2006-01-02T13:37:42",
		"2006-01-02T13:37:42,326",
		// "2006-01-02T13:37:42.Z",
		"2006:01-02T13:37:42Z",
		"2006-01:02T13:37:42Z",
		"2006-01-02 13:37:42Z",
		"2006-01-02T13-37:42Z",
		"2006-01-02T13:37-42Z",
		"200a-01-02T13:37:42Z",
		"2006-0b-02T13:37:42Z",
		"2006-01-0cT13:37:42Z",
		"2006-01-02T1d:37:42Z",
		"2006-01-02T13:3e:42Z",
		"2006-01-02T13:37:4fZ",
		"2006-01-02T13:37:42.727",
		"2006-01-02T13:37:42x08:00",
		"2006-01-02T13:37:42+08x00",
		"2006-01-02T13:37:42+0a:00",
		"2006-01-02T13:37:42+08:0a",
		"2006-01-02T13:37:42+08:0",
		"2006-01-02T13:37:42+08:00hello",
		"2006-01-02§13:37:42+08:00",
	}

	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			if _, err := time.Parse(time.RFC3339, test); err == nil {
				t.Errorf("%q parsed OK for the standard library", test)
			}

			if _, err := parseTime(test); err == nil {
				t.Errorf("%q parsed OK", test)
			}
		})
	}
}
