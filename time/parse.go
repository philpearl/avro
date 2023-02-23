package time

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// parseTime parses an RFC3339 timestamp. It exists because custom parsing of
// this particular timezone is faster than using time.Parse, and parsing string
// timestamps comes up rather more often than is ideal
func parseTime(in string) (time.Time, error) {
	if len(in) < 10 {
		return time.Time{}, fmt.Errorf("expect time string to be at least 10 characters long %q", in)
	}
	if in[4] != '-' || in[7] != '-' {
		return time.Time{}, fmt.Errorf("date not formatted as expected, missing -")
	}

	// "2006-01-02T15:04:05Z07:00"
	y, err := atoi4(in[:4])
	if err != nil {
		return time.Time{}, fmt.Errorf("could not parse year %q: %w", in[:4], err)
	}
	m, err := atoi2(in[5:7])
	if err != nil {
		return time.Time{}, fmt.Errorf("could not parse month %q: %w", in[5:7], err)
	}
	d, err := atoi2(in[8:10])
	if err != nil {
		return time.Time{}, fmt.Errorf("could not parse day %q: %w", in[8:10], err)
	}

	if len(in) == 10 {
		return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), nil
	}

	if len(in) < 20 {
		return time.Time{}, fmt.Errorf("expect time string to be at least 20 characters long if greater than 10 characters %q", in)
	}

	if in[10] != 'T' {
		return time.Time{}, fmt.Errorf("time not formatted as expected, missing T")
	}

	if in[13] != ':' || in[16] != ':' {
		return time.Time{}, fmt.Errorf("time not formatted as expected, missing ':': %q", in)
	}

	h, err := atoi2(in[11:13])
	if err != nil {
		return time.Time{}, fmt.Errorf("could not parse hour %q: %w", in[11:13], err)
	}
	min, err := atoi2(in[14:16])
	if err != nil {
		return time.Time{}, fmt.Errorf("could not parse minute %q: %w", in[14:16], err)
	}
	s, err := atoi2(in[17:19])
	if err != nil {
		return time.Time{}, fmt.Errorf("could not parse seconds %q: %w", in[17:19], err)
	}

	remaining := in[19:]
	c := remaining[0]

	var nsec int
	if c == '.' || c == ',' {
		remaining = remaining[1:]
		// Fractional seconds!
		var val, i int
		var c rune
		var mult int = 1e9
		for i, c = range remaining {
			if c >= '0' && c <= '9' {
				val = val*10 + int(c-'0')
				mult /= 10
			} else {
				i -= 1
				break
			}
		}
		nsec = val * mult
		remaining = remaining[i+1:]
		if len(remaining) == 0 {
			return time.Time{}, fmt.Errorf("too short to contain timezone")
		}
	}

	c = remaining[0]
	remaining = remaining[1:]
	var tz *time.Location
	if c == 'Z' {
		tz = time.UTC
	} else {
		var sign int
		switch c {
		case '+':
			sign = 1
		case '-':
			sign = -1
		default:
			return time.Time{}, fmt.Errorf("TZ must start with + or -, not %c", c)
		}
		if len(remaining) < 5 {
			return time.Time{}, fmt.Errorf("TZ info wrong length")
		}
		if remaining[2] != ':' {
			return time.Time{}, fmt.Errorf("TZ info does not include ':'")
		}
		tzh, err := atoi2(remaining[:2])
		if err != nil {
			return time.Time{}, fmt.Errorf("could not parse timezone offset hours %q: %w", remaining[:2], err)
		}
		tzm, err := atoi2(remaining[3:5])
		if err != nil {
			return time.Time{}, fmt.Errorf("could not parse timezone offset minutes %q: %w", remaining[3:5], err)
		}

		tz = getTimezone(sign * (tzh*60*60 + tzm*60))

		remaining = remaining[5:]
	}

	if len(remaining) != 0 {
		return time.Time{}, fmt.Errorf("unparsed data remains after parsing complete (%q)", remaining)
	}

	return time.Date(y, time.Month(m), d, h, min, s, nsec, tz), nil
}

var (
	tzLock sync.Mutex
	tzMap  = make(map[int]*time.Location)
)

func getTimezone(offset int) *time.Location {
	tzLock.Lock()
	defer tzLock.Unlock()
	tz, ok := tzMap[offset]
	if !ok {
		tz = time.FixedZone("", offset)
		tzMap[offset] = tz
	}
	return tz
}

var errCannotParseNumber = errors.New("couldn't parse number")

func atoi2(in string) (int, error) {
	_ = in[1]
	a, b := int(in[0]-'0'), int(in[1]-'0')
	if a < 0 || a > 9 || b < 0 || b > 9 {
		return 0, errCannotParseNumber
	}
	return a*10 + b, nil
}

func atoi4(in string) (int, error) {
	_ = in[3]
	a, b, c, d := int(in[0]-'0'), int(in[1]-'0'), int(in[2]-'0'), int(in[3]-'0')
	if a < 0 || a > 9 || b < 0 || b > 9 || c < 0 || c > 9 || d < 0 || d > 9 {
		return 0, errCannotParseNumber
	}
	return a*1000 + b*100 + c*10 + d, nil
}
