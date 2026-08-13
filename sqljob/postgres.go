package sqljob

import (
	"strings"
	"unicode"
)

// pgSplitter 处理 PostgreSQL/Greenplum/Oracle 的语句拆分：以顶层分号 ';' 分隔，
// 并正确跳过单引号字符串、双引号标识符、行注释(--)、块注释(/* */)
// 与美元引用($tag$...$tag$) 内部的分号。
type pgSplitter struct{}

func (pgSplitter) Split(sqlText string) []string {
	var stmts []string
	var buf strings.Builder
	runes := []rune(sqlText)
	n := len(runes)

	flush := func() {
		if s := strings.TrimSpace(buf.String()); s != "" {
			stmts = append(stmts, s)
		}
		buf.Reset()
	}

	for i := 0; i < n; {
		c := runes[i]
		switch {
		case c == '-' && i+1 < n && runes[i+1] == '-':
			// 行注释，直到行尾
			for i < n && runes[i] != '\n' {
				buf.WriteRune(runes[i])
				i++
			}
		case c == '/' && i+1 < n && runes[i+1] == '*':
			// 块注释 /* ... */
			buf.WriteRune(runes[i])
			buf.WriteRune(runes[i+1])
			i += 2
			for i < n {
				if runes[i] == '*' && i+1 < n && runes[i+1] == '/' {
					buf.WriteRune(runes[i])
					buf.WriteRune(runes[i+1])
					i += 2
					break
				}
				buf.WriteRune(runes[i])
				i++
			}
		case c == '\'':
			i = consumeQuoted(runes, i, '\'', &buf)
		case c == '"':
			i = consumeQuoted(runes, i, '"', &buf)
		case c == '$':
			if tag, ok := readDollarTag(runes, i); ok {
				i = consumeDollar(runes, i, tag, &buf)
			} else {
				buf.WriteRune(c)
				i++
			}
		case c == ';':
			flush()
			i++
		default:
			buf.WriteRune(c)
			i++
		}
	}
	flush()
	return stmts
}

// consumeQuoted 从开引号处开始，写入整个带引号的串（含成对的转义引号 '' 或 ""），返回结束后的下标。
func consumeQuoted(runes []rune, i int, quote rune, buf *strings.Builder) int {
	n := len(runes)
	buf.WriteRune(runes[i]) // 开引号
	i++
	for i < n {
		buf.WriteRune(runes[i])
		if runes[i] == quote {
			// 成对引号是转义，不结束
			if i+1 < n && runes[i+1] == quote {
				buf.WriteRune(runes[i+1])
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

// readDollarTag 尝试从 runes[i]=='$' 处读取美元引用标签（$$ 或 $ident$）。
// 标签首字符不能是数字，以避免把位置参数 $1 误判为标签。
func readDollarTag(runes []rune, i int) ([]rune, bool) {
	n := len(runes)
	j := i + 1
	if j < n && runes[j] == '$' {
		return runes[i : j+1], true // $$
	}
	for j < n {
		c := runes[j]
		if c == '$' {
			return runes[i : j+1], true
		}
		if c == '_' || unicode.IsLetter(c) || (j > i+1 && unicode.IsDigit(c)) {
			j++
			continue
		}
		return nil, false
	}
	return nil, false
}

// consumeDollar 写入整个美元引用块（含开闭标签），返回结束后的下标。
func consumeDollar(runes []rune, i int, tag []rune, buf *strings.Builder) int {
	n := len(runes)
	for _, r := range tag { // 开标签
		buf.WriteRune(r)
	}
	i += len(tag)
	for i < n {
		if matchAt(runes, i, tag) {
			for _, r := range tag { // 闭标签
				buf.WriteRune(r)
			}
			return i + len(tag)
		}
		buf.WriteRune(runes[i])
		i++
	}
	return i
}

// matchAt 判断 runes[i:] 是否以 tag 开头。
func matchAt(runes []rune, i int, tag []rune) bool {
	if i+len(tag) > len(runes) {
		return false
	}
	for k := range tag {
		if runes[i+k] != tag[k] {
			return false
		}
	}
	return true
}
