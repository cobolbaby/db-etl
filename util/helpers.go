package util

import (
	"regexp"
	"strings"
)

var controlChars = regexp.MustCompile(`[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]`)

func SanitizeString(s string) string {
	if strings.ToUpper(strings.TrimSpace(s)) == "NULL" || strings.TrimSpace(s) == "" {
		return ""
	}

	s = controlChars.ReplaceAllString(s, "")

	if strings.Contains(s, `"`) {
		s = strings.ReplaceAll(s, `"`, `""`)
	}

	if strings.ContainsAny(s, ",\"\n") {
		return `"` + s + `"`
	}

	return s
}
