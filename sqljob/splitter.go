package sqljob

import "db-etl/config"

// statementSplitter 按数据库方言把一段 SQL 文本拆分为多个独立执行单元。
// 每个执行单元会独立提交，因此天然构成不同的事务。
type statementSplitter interface {
	// Split 返回按方言拆分后的语句列表（已 TrimSpace，空语句被丢弃）。
	Split(sqlText string) []string
}

// newSplitter 依据数据库类型返回对应方言的拆分器（工厂模式，与 reader/writer 一致）。
//   - SQL Server：以单独成行的 GO 作为批处理分隔符；
//   - PostgreSQL / Greenplum / Oracle：以顶层分号 ';' 分隔（跳过字符串/注释/美元引用内部）。
func newSplitter(dbType config.DBType) statementSplitter {
	switch dbType {
	case config.DBTypeMSSQL:
		return mssqlSplitter{}
	default:
		return pgSplitter{}
	}
}
