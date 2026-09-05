package reader

import (
	"encoding/hex"
	"fmt"
	"time"
)

// TimestampLayout 是时间值的规范文本形式。
// 保留至纳秒且自动去除尾随零，可覆盖 Oracle TIMESTAMP(9) 与 MSSQL DATETIME2(7)，
// 同时是 PostgreSQL / Oracle / MSSQL 都能直接解析的字面量格式。
const TimestampLayout = "2006-01-02 15:04:05.999999999"

// ColumnKind 是跨源库归一化的列语义类别，也是 reader 与 writer 之间关于「值长什么样」的唯一约定。
//
// 各源库的类型名千差万别（INT4 / BIGINT / NUMBER），由各 reader 方言负责映射到此枚举；
// 各目标格式的落地方式也各不相同（COPY 文本 / parquet 物理类型），由各 writer 自行决定。
// 两侧都只依赖本枚举，不需要知道对方的存在。
type ColumnKind int

const (
	// KindString 覆盖文本类，以及为避免精度损失而按文本传递的 NUMERIC/DECIMAL/NUMBER。
	// 驱动返回 string 或 []byte。
	KindString ColumnKind = iota
	// KindInt 为整数类，驱动返回 int64 等有符号整型。
	KindInt
	// KindFloat 为浮点类，驱动返回 float64/float32。
	KindFloat
	// KindBool 为布尔类，驱动返回 bool 或 0/1 整数（如 MSSQL BIT）。
	KindBool
	// KindTime 为日期时间类，驱动返回 time.Time。
	KindTime
	// KindBytes 为二进制类（BLOB/RAW/VARBINARY），驱动返回 []byte。
	KindBytes
)

// String 返回可读的类别名，供日志与错误信息使用。
func (k ColumnKind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindInt:
		return "int"
	case KindFloat:
		return "float"
	case KindBool:
		return "bool"
	case KindTime:
		return "time"
	case KindBytes:
		return "bytes"
	default:
		return "unknown"
	}
}

// ColumnMeta 描述查询结果中的一列，是列信息的唯一来源；
// 三个字段同源于一次列类型探测，故合为一个结构体按列返回。
type ColumnMeta struct {
	// Name 为结果集列名（含 fields_mapping 重命名后的别名）。
	Name string
	// TypeName 为源库原始类型名（如 INT4、DATETIME2），保留用于诊断与日志。
	TypeName string
	// Kind 为归一化后的语义类别，writer 据此决定落地方式。
	Kind ColumnKind
}

// ValueNormalizer 修正驱动层的值表示差异，使同一 ColumnKind 在各源库上呈现一致的 Go 类型。
// 仅少数类型需要（如 MSSQL uniqueidentifier 的混合字节序），其余列为 nil。
type ValueNormalizer func(any) any

// NaiveTimeNormalizer 返回一个把「无时区时间列」的墙钟重新贴上 loc 时区的修正函数。
//
// 各驱动对 TIMESTAMP / DATETIME2 等无时区列，返回的是「数据库里的墙钟数字 + UTC Location」，
// 并非真正的 UTC 瞬时。若直接按 UTC 归一（如 parquet 的 t.UTC().UnixMilli()），
// 就把墙钟当成了 UTC，写出的绝对时刻会整体偏移「loc 与 UTC 的时差」。
// 此处仅替换 Location 为 loc、保持墙钟数字不变，使墙钟被解释为源库所在时区，
// 从而携带正确的绝对时刻；而按墙钟文本落地的路径（FormatText）渲染结果不变，不受影响。
//
// loc 由源库配置的 timezone 显式提供（见 config.DBConfig.Location），
// 因为运行节点与数据库节点时区可能不同，不能依赖运行节点本地时区。
//
// 带时区列（TIMESTAMPTZ / DATETIMEOFFSET / TIMESTAMP WITH TIME ZONE 等）驱动已返回正确瞬时，
// 不应走此修正，否则会二次偏移。
func NaiveTimeNormalizer(loc *time.Location) ValueNormalizer {
	if loc == nil {
		loc = time.Local
	}
	return func(v any) any {
		t, ok := v.(time.Time)
		if !ok {
			return v
		}
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc)
	}
}

// FormatText 将值渲染为规范文本形式：只做「Go 值 → 字符串」，
// 不含任何目标格式的转义或 NULL 表示，那些由各 writer 在此结果之上叠加。
// nil 返回空串，调用方应先自行区分 NULL。
func FormatText(kind ColumnKind, v any) string {
	if v == nil {
		return ""
	}

	switch t := v.(type) {
	case string:
		return t
	case []byte:
		if kind == KindBytes {
			return hex.EncodeToString(t)
		}
		// 文本类列的 []byte 是字节形式的字符串（如 MSSQL 的 VARCHAR、PG 的数组字面量）。
		return string(t)
	case time.Time:
		return t.Format(TimestampLayout)
	default:
		return fmt.Sprint(t)
	}
}
