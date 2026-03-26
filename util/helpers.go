package util

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

type ColHandler func(any) string

var controlChars = regexp.MustCompile(`[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]`)

func MSSQLUUIDToString(b []byte) (string, error) {
	if len(b) != 16 {
		return "", fmt.Errorf("invalid uuid length")
	}
	u := []byte{
		b[3], b[2], b[1], b[0],
		b[5], b[4],
		b[7], b[6],
		b[8], b[9],
		b[10], b[11], b[12], b[13], b[14], b[15],
	}
	id, _ := uuid.FromBytes(u)
	return strings.ToUpper(id.String()), nil
}

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

func GetColHandler(dbType string) ColHandler {
	switch strings.ToUpper(dbType) {
	case "UNIQUEIDENTIFIER":
		return func(v any) string {
			switch t := v.(type) {
			case []byte:
				s, _ := MSSQLUUIDToString(t)
				return s
			case string:
				return strings.ToUpper(t)
			default:
				return ""
			}
		}
	case "DATETIME", "DATETIME2", "DATE", "TIME":
		return func(v any) string {
			if t, ok := v.(time.Time); ok && !t.IsZero() {
				return t.Format("2006-01-02 15:04:05")
			}
			return ""
		}
	default:
		return func(v any) string {
			if v == nil {
				return ""
			}
			switch t := v.(type) {
			case []byte:
				return SanitizeString(string(t))
			case string:
				return SanitizeString(t)
			default:
				return SanitizeString(fmt.Sprintf("%v", t))
			}
		}
	}
}
