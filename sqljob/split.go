package sqljob

import "strings"

// splitStatements 将一段 SQL 脚本切分为多条独立语句（以 `;` 为分隔符）。
//
// 切分时会正确跳过下列上下文中的分号，避免误切：
//   - 行注释 `-- ...`
//   - 块注释 `/* ... */`（支持 PostgreSQL 的嵌套块注释）
//   - 单引号字符串 '...'（`”` 视为转义引号）
//   - 双引号标识符 "..."（`""` 视为转义）
//   - 美元引用 $tag$ ... $tag$（PostgreSQL 函数体常用）
//
// 返回的每条语句均已去除首尾空白，且不含末尾分号；空语句（纯注释/空白）被丢弃。
func splitStatements(script string) []string {
	var (
		statements []string
		buf        strings.Builder
		runes      = []rune(script)
		n          = len(runes)
		// meaningful 标记当前缓冲区是否含有实际 SQL（非注释、非空白），
		// 用于丢弃“仅注释/空白”的片段（如文件末尾的 `-- done`），避免把它们当语句执行。
		meaningful bool
	)

	flush := func() {
		if meaningful {
			if stmt := strings.TrimSpace(buf.String()); stmt != "" {
				statements = append(statements, stmt)
			}
		}
		buf.Reset()
		meaningful = false
	}

	for i := 0; i < n; {
		c := runes[i]

		switch {
		// 行注释：-- 到行尾
		case c == '-' && i+1 < n && runes[i+1] == '-':
			for i < n && runes[i] != '\n' {
				buf.WriteRune(runes[i])
				i++
			}

		// 块注释：/* ... */，支持嵌套
		case c == '/' && i+1 < n && runes[i+1] == '*':
			depth := 1
			buf.WriteRune(runes[i])
			buf.WriteRune(runes[i+1])
			i += 2
			for i < n && depth > 0 {
				if runes[i] == '/' && i+1 < n && runes[i+1] == '*' {
					depth++
					buf.WriteRune(runes[i])
					buf.WriteRune(runes[i+1])
					i += 2
				} else if runes[i] == '*' && i+1 < n && runes[i+1] == '/' {
					depth--
					buf.WriteRune(runes[i])
					buf.WriteRune(runes[i+1])
					i += 2
				} else {
					buf.WriteRune(runes[i])
					i++
				}
			}

		// 单引号字符串
		case c == '\'':
			meaningful = true
			buf.WriteRune(c)
			i++
			for i < n {
				if runes[i] == '\'' {
					// '' 转义
					if i+1 < n && runes[i+1] == '\'' {
						buf.WriteRune(runes[i])
						buf.WriteRune(runes[i+1])
						i += 2
						continue
					}
					buf.WriteRune(runes[i])
					i++
					break
				}
				buf.WriteRune(runes[i])
				i++
			}

		// 双引号标识符
		case c == '"':
			meaningful = true
			buf.WriteRune(c)
			i++
			for i < n {
				if runes[i] == '"' {
					if i+1 < n && runes[i+1] == '"' {
						buf.WriteRune(runes[i])
						buf.WriteRune(runes[i+1])
						i += 2
						continue
					}
					buf.WriteRune(runes[i])
					i++
					break
				}
				buf.WriteRune(runes[i])
				i++
			}

		// 美元引用 $tag$ ... $tag$
		case c == '$':
			meaningful = true
			if tag, ok := dollarTag(runes, i); ok {
				// 写入起始 tag
				buf.WriteString(tag)
				i += len([]rune(tag))
				// 查找匹配的结束 tag
				closeAt := indexOfTag(runes, i, tag)
				if closeAt < 0 {
					// 未闭合：把剩余内容全部当作字符串写入
					for i < n {
						buf.WriteRune(runes[i])
						i++
					}
				} else {
					for i < closeAt {
						buf.WriteRune(runes[i])
						i++
					}
					buf.WriteString(tag)
					i += len([]rune(tag))
				}
			} else {
				buf.WriteRune(c)
				i++
			}

		// 语句分隔符
		case c == ';':
			flush()
			i++

		default:
			if !isSpace(c) {
				meaningful = true
			}
			buf.WriteRune(c)
			i++
		}
	}

	flush()
	return statements
}

// isSpace 判断是否为 SQL 语句间可忽略的空白字符。
func isSpace(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}

// dollarTag 检测 runes[start] 处是否为一个合法的美元引用起始 tag（如 `$$` 或 `$body$`）。
// PostgreSQL 规则：tag 可为空，或为一个不含美元符号、且不以数字开头的标识符。
func dollarTag(runes []rune, start int) (string, bool) {
	n := len(runes)
	if start >= n || runes[start] != '$' {
		return "", false
	}
	j := start + 1
	for j < n {
		r := runes[j]
		if r == '$' {
			tag := string(runes[start : j+1])
			// tag 不能以数字开头（$1 之类是参数占位符，不是美元引用）
			if len(tag) > 2 { // 形如 $x...$
				first := runes[start+1]
				if first >= '0' && first <= '9' {
					return "", false
				}
			}
			return tag, true
		}
		isIdent := r == '_' ||
			(r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9')
		if !isIdent {
			return "", false
		}
		j++
	}
	return "", false
}

// indexOfTag 从 from 开始查找 tag 在 runes 中首次出现的下标，找不到返回 -1。
func indexOfTag(runes []rune, from int, tag string) int {
	tagRunes := []rune(tag)
	tn := len(tagRunes)
	for i := from; i+tn <= len(runes); i++ {
		match := true
		for k := 0; k < tn; k++ {
			if runes[i+k] != tagRunes[k] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
