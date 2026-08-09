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
	s := &splitter{runes: []rune(script)}
	return s.run()
}

// splitter 逐字符扫描 SQL 脚本，识别注释/字符串/美元引用等上下文，
// 只在“裸露”的分号处切分语句。
type splitter struct {
	runes []rune
	pos   int             // 当前扫描位置
	buf   strings.Builder // 当前正在累积的语句
	// meaningful 标记 buf 是否含有实际 SQL（非注释、非空白），
	// 用于丢弃“仅注释/空白”的片段（如文件末尾的 `-- done`）。
	meaningful bool
	out        []string
}

// run 驱动扫描主循环：每一步识别当前上下文并交给对应的 consume 方法处理。
func (s *splitter) run() []string {
	for s.pos < len(s.runes) {
		switch {
		case s.hasPrefix("--"):
			s.consumeLineComment()
		case s.hasPrefix("/*"):
			s.consumeBlockComment()
		case s.peek() == '\'' || s.peek() == '"':
			s.consumeQuoted(s.peek())
		case s.peek() == '$':
			s.consumeDollar()
		case s.peek() == ';':
			s.flush()
			s.pos++
		default:
			if !isSpace(s.peek()) {
				s.meaningful = true
			}
			s.emit(1)
		}
	}
	s.flush()
	return s.out
}

// peek 返回当前位置的字符（调用方需自行保证未越界）。
func (s *splitter) peek() rune { return s.runes[s.pos] }

// hasPrefix 判断当前位置是否以 p 开头。
func (s *splitter) hasPrefix(p string) bool {
	pr := []rune(p)
	if s.pos+len(pr) > len(s.runes) {
		return false
	}
	for i, r := range pr {
		if s.runes[s.pos+i] != r {
			return false
		}
	}
	return true
}

// emit 将接下来的 n 个字符写入当前语句并前进相应位置。
func (s *splitter) emit(n int) {
	for k := 0; k < n && s.pos < len(s.runes); k++ {
		s.buf.WriteRune(s.runes[s.pos])
		s.pos++
	}
}

// flush 结束当前语句：去除首尾空白后，若含实际内容则收集，并重置状态。
func (s *splitter) flush() {
	if s.meaningful {
		if stmt := strings.TrimSpace(s.buf.String()); stmt != "" {
			s.out = append(s.out, stmt)
		}
	}
	s.buf.Reset()
	s.meaningful = false
}

// consumeLineComment 消费 `-- ...` 直到行尾（换行符留给主循环处理）。
func (s *splitter) consumeLineComment() {
	for s.pos < len(s.runes) && s.peek() != '\n' {
		s.emit(1)
	}
}

// consumeBlockComment 消费 `/* ... */`，支持 PostgreSQL 的嵌套块注释。
func (s *splitter) consumeBlockComment() {
	depth := 0
	for s.pos < len(s.runes) {
		switch {
		case s.hasPrefix("/*"):
			depth++
			s.emit(2)
		case s.hasPrefix("*/"):
			depth--
			s.emit(2)
			if depth == 0 {
				return
			}
		default:
			s.emit(1)
		}
	}
}

// consumeQuoted 消费以 q 为界的字符串/标识符；连续两个 q（如 ” 或 ""）视为转义。
// 单引号字符串与双引号标识符的规则一致，故共用此方法。
func (s *splitter) consumeQuoted(q rune) {
	s.meaningful = true
	s.emit(1) // 起始引号
	for s.pos < len(s.runes) {
		if s.peek() == q {
			// 连续两个引号是转义，否则即为结束引号
			if s.pos+1 < len(s.runes) && s.runes[s.pos+1] == q {
				s.emit(2)
				continue
			}
			s.emit(1)
			return
		}
		s.emit(1)
	}
}

// consumeDollar 处理美元引用 $tag$ ... $tag$；
// 若当前 `$` 不是合法起始 tag（如参数占位符 $1），则按普通字符处理。
func (s *splitter) consumeDollar() {
	s.meaningful = true
	tag, ok := dollarTag(s.runes, s.pos)
	if !ok {
		s.emit(1)
		return
	}
	tagLen := len([]rune(tag))
	s.emit(tagLen) // 起始 tag

	closeAt := indexOfTag(s.runes, s.pos, tag)
	if closeAt < 0 {
		// 未闭合：把剩余内容全部并入当前语句
		s.emit(len(s.runes) - s.pos)
		return
	}
	s.emit(closeAt - s.pos) // tag 之间的内容
	s.emit(tagLen)          // 结束 tag
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
