package timing

import (
	"math/rand"
	"time"
)

// UnixMilli returns t as a Unix time, the number of nanoseconds elapsed since
// January 1, 1970 UTC. The result is undefined if the Unix time in nanoseconds
// in cannot be represented by an int64 (a date before the year 1678 or after
// 2262). Note that this means the result of calling UnixMilli on the zero Time
// on the zero Time is undefined. The result does not depend on the location
// associated with t.
func UnixMilli(t time.Time) int64 {
	return t.UnixNano() / 1e6
}

// JitterInterval returns a duration that some value between the
// interval and 2x the interval.
func JitterInterval(interval time.Duration) time.Duration {
	return time.Duration(rand.Float64()*float64(interval)) + interval
}

// RoundPartOfDay produces a time value with the hour value
// rounded down to the most recent interval.
func RoundPartOfDay(n int) time.Time { return findPartHour(time.Now(), n) }

// RoundPartOfHour produces a time value with the minute value
// rounded down to the most recent interval.
func RoundPartOfHour(n int) time.Time { return findPartMin(time.Now(), n) }

// RoundPartOfMinute produces a time value with the second value
// rounded down to the most recent interval.
func RoundPartOfMinute(n int) time.Time { return findPartSec(time.Now(), n) }

// this implements the logic of RoundPartOfDay, but takes time as an
// argument for testability.
func findPartHour(now time.Time, num int) time.Time {
	var hour int

	if num > now.Hour() || num > 12 || num <= 0 {
		hour = 0
	} else {
		hour = now.Hour() - (now.Hour() % num)
	}

	return time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, time.UTC)
}

// this implements the logic of RoundPartOfHour, but takes time as an
// argument for testability.
func findPartMin(now time.Time, num int) time.Time {
	var min int

	if num > now.Minute() || num > 30 || num <= 0 {
		min = 0
	} else {
		min = now.Minute() - (now.Minute() % num)
	}

	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), min, 0, 0, time.UTC)
}

// this implements the logic of RoundPartOfMinute, but takes time as an
// argument for testability.
func findPartSec(now time.Time, num int) time.Time {
	var sec int

	if num > now.Second() || num > 30 || num <= 0 {
		sec = 0
	} else {
		sec = now.Second() - (now.Second() % num)
	}

	return time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), sec, 0, time.UTC)
}

// Creates and returns a time.Time corresponding to the start of the UTC day containing the given date.
func GetUTCDay(date time.Time) time.Time {
	// Convert to UTC.
	date = date.In(time.UTC)
	// Create a new time.Time for the beginning of the day.
	year, month, day := date.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// Creates and returns a time.Time corresponding to the start of the UTC hour containing the given date.
func GetUTCHour(date time.Time) time.Time {
	// Convert to UTC.
	date = date.In(time.UTC)
	// Create a new time.Time for the beginning of the hour.
	year, month, day := date.Date()
	hour := date.Hour()
	return time.Date(year, month, day, hour, 0, 0, 0, time.UTC)
}
