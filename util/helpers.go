package util

import (
	"regexp"
	"strings"
)

const NullSentinel = "__DB_ETL_NULL__"

var controlChars = regexp.MustCompile(`[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]`)

// SanitizeString 对字符串进行 CSV 安全转义，同时区分 NULL 和空字符串。
//
// 处理流程：
//
//  1. 空白判定（优先）：原始值 TrimSpace 为空则直接返回 ""
//     注意：故意在控制字符清洗之前判断，若源库字段只含控制字符（如 \x00），
//     在业务上仍视为有效非空值，由后续步骤清洗后继续走 CSV 转义流程。
//
//  2. 保护保留的 NULL 哨兵值（在清洗之前拦截）：
//     nil 值由上层 defaultColumnHandler 编码为 NullSentinel 后才进入此函数，
//     此时哨兵值不应被清洗逻辑修改，提前拦截并强制加引号，
//     确保作为普通文本入库，而非被 COPY 识别为 SQL NULL。
//
// 3. 移除不可见控制字符（\x00-\x1F 等），避免破坏 CSV 格式。
//
// 4. CSV 字段转义：
//   - 若含有双引号，替换为两个双引号（CSV 标准转义规则）
//   - 若含有逗号、双引号或换行符，用双引号包裹整个字段
//   - 否则返回原文本，无需包装
//
// 背景：通过 util.NullSentinel（"__DB_ETL_NULL__"）区分 nil 与空字符串：
//   - nil 值被 reader 编码为 NullSentinel
//   - 在 PostgreSQL COPY 中，NULL '__DB_ETL_NULL__' 配置会将哨兵识别为 SQL NULL
//   - 空字符串保持为 ""，在 COPY 中保持为空字符串（不是 NULL）
//   - 若源数据恰好包含哨兵文本，会被强制加引号保护，确保作为普通文本入库
func SanitizeString(s string) string {
	// Step 1: 空白判定（优先，保留原始语义）
	// 在控制字符清洗之前判断，若源库字段只含控制字符仍视为有效非空值，
	// 由后续步骤清洗后继续走 CSV 转义流程
	if strings.TrimSpace(s) == "" {
		return ""
	}

	// Step 2: 保护保留的 NULL 哨兵值（在清洗之前拦截）
	// nil 值由上层 defaultColumnHandler 编码为 NullSentinel 后才进入此函数，
	// 此时哨兵值不应被清洗逻辑修改，提前拦截并强制加引号，确保作为普通文本入库，而非被 COPY 识别为 SQL NULL
	if s == NullSentinel {
		return `"` + s + `"`
	}

	// Step 3: 清洗控制字符，避免破坏 CSV 格式
	s = controlChars.ReplaceAllString(s, "")

	// Step 4: CSV 字段转义
	if strings.Contains(s, `"`) {
		s = strings.ReplaceAll(s, `"`, `""`)
	}

	if strings.ContainsAny(s, ",\"\n") {
		return `"` + s + `"`
	}

	return s
}
