package queue

import (
	"strings"
)

func addJobsSuffix(s string) string {
	return s + ".jobs"
}

func trimJobsSuffix(s string) string {
	return strings.TrimSuffix(s, ".jobs")
}

func addGroupSufix(s string) string {
	return s + ".group"
}
