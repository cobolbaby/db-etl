package sqljob

import (
	"strconv"
	"strings"
)

// mssqlSplitter 处理 SQL Server 的语句拆分：以单独成行的 GO 作为批处理分隔符
// （GO 本身不是 T-SQL 语句）。批处理内部不以分号拆分。
// 支持 "GO" 与带次数的 "GO 3"（大小写不敏感）。
type mssqlSplitter struct{}

func (mssqlSplitter) Split(sqlText string) []string {
	var batches []string
	var buf strings.Builder

	flush := func() {
		if s := strings.TrimSpace(buf.String()); s != "" {
			batches = append(batches, s)
		}
		buf.Reset()
	}

	for _, line := range strings.Split(sqlText, "\n") {
		if isGoSeparator(strings.TrimSpace(line)) {
			flush()
			continue
		}
		buf.WriteString(line)
		buf.WriteString("\n")
	}
	flush()
	return batches
}

// isGoSeparator 判断一行是否为 GO 批处理分隔符（可带执行次数，如 "GO 5"）。
func isGoSeparator(line string) bool {
	fields := strings.Fields(line)
	if len(fields) == 0 || !strings.EqualFold(fields[0], "GO") {
		return false
	}
	if len(fields) == 1 {
		return true
	}
	if len(fields) == 2 {
		if _, err := strconv.Atoi(fields[1]); err == nil {
			return true
		}
	}
	return false
}
