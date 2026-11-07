package writer

import (
	"db-etl/reader"
	"encoding/hex"
	"regexp"
	"strings"
	"time"
)

// nullSentinel 是 COPY 用来识别 SQL NULL 的哨兵文本（见 buildCopySQL 的 NULL 选项）。
//
// COPY 的 CSV 格式无法区分「空字符串」与「NULL」——两者都呈现为空字段。
// 改用哨兵后二者得以分开：nil 编码为哨兵被识别为 NULL（能正确触发 NOT NULL 约束），
// 空字符串仍编码为空字段并如实入库。
const nullSentinel = "__DB_ETL_NULL__"

// controlChars 匹配会破坏 CSV 结构的不可见控制字符（保留 \t \n \r 交由引号包裹处理）。
var controlChars = regexp.MustCompile(`[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]`)

// encodeCopyValue 将驱动返回的原始值编码为 COPY ... FORMAT CSV 的字段文本。
// 这是 PostgreSQL 目标独有的落地格式，因此与 reader / transform 无关。
//
// loc 为目标库会话时区（见 pgWriterDialect.loc），COPY 写入的时间戳文本不带时区，
// PostgreSQL 按会话 TimeZone 解析；reader 侧各源库可能配置不同时区，若不统一换算，
// 不同源的同一批数据落地后文本所代表的时区基准会不一致。因此在渲染文本前，
// 将 time.Time 统一换算（.In(loc)，不改变其绝对时刻）到目标时区，
// 使生成的文本恰好是目标会话 TimeZone 下应解析出原始绝对时刻的墙钟。
func encodeCopyValue(kind reader.ColumnKind, v any, loc *time.Location) string {
	if v == nil {
		return nullSentinel
	}

	// bytea 在 COPY CSV 中使用 \x 十六进制字面量，且不含需转义的字符。
	if kind == reader.KindBytes {
		if b, ok := v.([]byte); ok {
			return `\x` + hex.EncodeToString(b)
		}
	}

	if kind == reader.KindTime && loc != nil {
		if t, ok := v.(time.Time); ok {
			v = t.In(loc)
		}
	}

	return sanitizeCSV(reader.FormatText(kind, v))
}

// sanitizeCSV 对字段文本做 CSV 转义，并保证不会与 nullSentinel 混淆。
//
// 处理顺序有讲究：
//  1. 先基于原始文本判空白（strings.TrimSpace），必须在清洗控制字符之前：
//     像 " \x01 " 这类夹杂控制字符与空格的字段，若先剥掉控制字符只剩两边的空格，
//     再判断就会被误判为全空白而清空，丢失这两个空格；基于原始文本判断则不会。
//     注意 \v（\x0B）、\f（\x0C）本身也被 TrimSpace 视为空白，仅含它们的字段
//     无论先后判断都会被清空，不属于本条要规避的场景。
//  2. 再拦截哨兵：源数据恰好等于哨兵文本时强制加引号，
//     确保它作为普通文本入库而非被 COPY 当成 NULL。
//  3. 清洗控制字符，避免破坏 CSV 结构。
//  4. 常规 CSV 转义。
func sanitizeCSV(s string) string {
	if strings.TrimSpace(s) == "" {
		return ""
	}

	if s == nullSentinel {
		return `"` + s + `"`
	}

	s = controlChars.ReplaceAllString(s, "")

	if strings.Contains(s, `"`) {
		s = strings.ReplaceAll(s, `"`, `""`)
	}

	// \r 必须一并判断，否则残留的裸回车会触发 COPY 报
	// "unquoted carriage return found in data"。
	if strings.ContainsAny(s, ",\"\n\r") {
		return `"` + s + `"`
	}

	return s
}
