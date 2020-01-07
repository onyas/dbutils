package dbutils

import (
	"database/sql"
	"fmt"
	//_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/op/go-logging"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

var log = logging.MustGetLogger("dbutils.log")
var DbPool *sync.Pool
var GlobalDb *sql.DB

type Database struct {
	source      string  // 数据库源
	driver      string  // 数据库驱动
	fields      string  // 字段
	tableName   string  // 表名
	whereStr    string  // where语句
	limitNumber string  // 限制条数
	orderBy     string  // 排序条件
	execStr     string  // 执行sql语句
	conn        *sql.DB // 数据库连接
}

type DbCfg struct {
	Source, Driver string
}

// 初始化连接池
func init() {
	database := Database{}
	database.source = DBConfig().Source
	database.driver = DBConfig().Driver
	db, err := sql.Open(database.driver, database.source)
	db.SetMaxOpenConns(2000)             // 最大链接
	db.SetMaxIdleConns(1000)             // 空闲连接，也就是连接池里面的数量
	db.SetConnMaxLifetime(7 * time.Hour) // 设置最大生成周期是7个小时
	database.checkErr(err)
	GlobalDb = db
	log.Info("init connection success!!!")
}

/**
sql.Open函数实际上是返回一个连接池对象，不是单个连接。
在open的时候并没有去连接数据库，只有在执行query、exce方法的时候才会去实际连接数据库。
在一个应用中同样的库连接只需要保存一个sql.Open之后的db对象就可以了，不需要多次open。
*/
//func CreateConn() interface{} {
//	Database := Database{}
//	var cfg Config.Config
//	cfg = new(Config.Mysql)
//	Database.source = cfg.GetConfig()["source"].(string)
//	Database.driver = cfg.GetConfig()["driver"].(string)
//	db, err := sql.Open(Database.driver, Database.source)
//	db.SetMaxOpenConns(2000)  // 最大链接
//	db.SetMaxIdleConns(1000)  // 空间连接，也就是连接池里面的数量
//	Database.checkErr(err)
//	Database.conn = db
//	return db
//}

func (Postgres Database) GetConn() *Database {
	Postgres.conn = GlobalDb
	return &Postgres
}

func (Postgres *Database) Close() error {
	err := Postgres.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

/**
查询方法
*/
func (Postgres *Database) Select(tableName string, field []string) *Database {
	var allField string
	allField = strings.Join(field, ",")
	Postgres.fields = "select " + allField + " from " + tableName
	Postgres.tableName = tableName
	return Postgres
}

/**
where子句
*/
func (Postgres *Database) Where(cond map[string]string) *Database {
	var whereStr = ""
	if len(cond) != 0 {
		whereStr = " where "
		for key, value := range cond {
			if !strings.Contains(key, "=") && !strings.Contains(key, ">") && !strings.Contains(key, "<") {
				key += "="
			}
			whereStr += key + "'" + value + "'" + " AND "
		}
	}
	// 删除所有字段最后一个,
	whereStr = strings.TrimSuffix(whereStr, "AND ")
	Postgres.whereStr = whereStr
	return Postgres
}

func (Postgres *Database) Limit(number int) *Database {
	Postgres.limitNumber = " limit " + strconv.Itoa(number)
	return Postgres
}

func (Postgres *Database) OrderByString(orderString ...string) *Database {
	if len(orderString) > 2 || len(orderString) <= 0 {
		log.Fatal("传入参数错误")
	} else if len(orderString) == 1 {
		Postgres.orderBy = " ORDER BY " + orderString[0] + " ASC"
	} else {
		Postgres.orderBy = " ORDER BY " + orderString[0] + " " + orderString[1]
	}
	return Postgres
}

/**
更新方法
*/
func (Postgres Database) Update(tableName string, str map[string]string) int64 {
	var tempStr = ""
	var allValue []interface{}
	for key, value := range str {
		tempStr += key + "=" + "?" + ","
		allValue = append(allValue, value)
	}
	tempStr = strings.TrimSuffix(tempStr, ",")
	Postgres.execStr = "update " + tableName + " set " + tempStr
	var allStr = Postgres.execStr + Postgres.whereStr
	stmt, err := Postgres.conn.Prepare(allStr)
	Postgres.checkErr(err)
	res, err := stmt.Exec(allValue...)
	Postgres.checkErr(err)
	rows, err := res.RowsAffected()
	return rows

}

/**
删除方法
*/
func (Postgres Database) Delete(tableName string) int64 {
	var tempStr = ""
	tempStr = "delete from " + tableName + Postgres.whereStr
	fmt.Println(tempStr)
	stmt, err := Postgres.conn.Prepare(tempStr)
	Postgres.checkErr(err)
	res, err := stmt.Exec()
	Postgres.checkErr(err)
	rows, err := res.RowsAffected()
	return rows
}

/**
插入方法
*/
func (Postgres Database) Insert(tableName string, data map[string]interface{}) int64 {
	var allField = ""
	var allValue = ""
	var allTrueValue []interface{}
	if len(data) != 0 {
		for key, value := range data {
			allField += key + ","
			allValue += "?" + ","
			allTrueValue = append(allTrueValue, value)
		}
	}
	allValue = strings.TrimSuffix(allValue, ",")
	allField = strings.TrimSuffix(allField, ",")
	allValue = "(" + allValue + ")"
	allField = "(" + allField + ")"
	var theStr = "insert into " + tableName + " " + allField + " values " + allValue
	log.Debug(theStr)
	stmt, err := Postgres.conn.Prepare(theStr)
	Postgres.checkErr(err)
	res, err := stmt.Exec(allTrueValue...)
	if err != nil {
		fmt.Println(err.Error())
		return 0
	}
	Postgres.checkErr(err)
	id, err := res.LastInsertId()
	return id
}

/**
分页查询
*/
func (Postgres Database) Pagination(Page int, Limit int) map[string]interface{} {
	res := Postgres.GetConn().Select(Postgres.tableName, []string{"count(*) as count"}).QueryRow()
	count, _ := strconv.Atoi(res["count"])
	// 计算总页码数
	totalPage := int(math.Ceil(float64(count) / float64(Limit)))
	if Page > totalPage {
		Page = totalPage
	}
	if Page <= 0 {
		Page = 1
	}
	// 计算偏移量
	setOff := (Page - 1) * Limit
	queryStr := Postgres.fields + Postgres.whereStr + Postgres.orderBy + " limit " + strconv.Itoa(setOff) + "," + strconv.Itoa(Limit)
	rows, err := Postgres.conn.Query(queryStr)
	defer rows.Close()
	Postgres.checkErr(err)
	Column, err := rows.Columns()
	Postgres.checkErr(err)
	// 创建一个查询字段类型的slice
	values := make([]sql.RawBytes, len(Column))
	// 创建一个任意字段类型的slice
	scanArgs := make([]interface{}, len(values))
	// 创建一个slice保存所以的字段
	var allRows []interface{}
	for i := range values {
		// 把values每个参数的地址存入scanArgs
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		// 把存放字段的元素批量放进去
		err = rows.Scan(scanArgs...)
		Postgres.checkErr(err)
		tempRow := make(map[string]string, len(Column))
		for i, col := range values {
			var key = Column[i]
			tempRow[key] = string(col)
		}
		allRows = append(allRows, tempRow)
	}
	returnData := make(map[string]interface{})
	returnData["totalPage"] = totalPage
	returnData["currentPage"] = Page
	returnData["rows"] = allRows
	return returnData
}

func (Postgres Database) QueryAll() []map[string]string {
	var queryStr = Postgres.fields + Postgres.whereStr + Postgres.orderBy + Postgres.limitNumber
	rows, err := Postgres.conn.Query(queryStr)
	defer rows.Close()
	Postgres.checkErr(err)
	Column, err := rows.Columns()
	Postgres.checkErr(err)
	// 创建一个查询字段类型的slice
	values := make([]sql.RawBytes, len(Column))
	// 创建一个任意字段类型的slice
	scanArgs := make([]interface{}, len(values))
	// 创建一个slice保存所以的字段
	var allRows []map[string]string
	for i := range values {
		// 把values每个参数的地址存入scanArgs
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		// 把存放字段的元素批量放进去
		err = rows.Scan(scanArgs...)
		Postgres.checkErr(err)
		tempRow := make(map[string]string, len(Column))
		for i, col := range values {
			var key = Column[i]
			tempRow[key] = string(col)
		}
		allRows = append(allRows, tempRow)
	}
	return allRows
}

func (Postgres Database) ExecSql(queryStr string) []map[string]string {
	rows, err := Postgres.conn.Query(queryStr)
	defer rows.Close()
	Postgres.checkErr(err)
	Column, err := rows.Columns()
	Postgres.checkErr(err)
	// 创建一个查询字段类型的slice
	values := make([]sql.RawBytes, len(Column))
	// 创建一个任意字段类型的slice
	scanArgs := make([]interface{}, len(values))
	// 创建一个slice保存所以的字段
	var allRows []map[string]string
	for i := range values {
		// 把values每个参数的地址存入scanArgs
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		// 把存放字段的元素批量放进去
		err = rows.Scan(scanArgs...)
		Postgres.checkErr(err)
		tempRow := make(map[string]string, len(Column))
		for i, col := range values {
			var key = Column[i]
			tempRow[key] = string(col)
		}
		allRows = append(allRows, tempRow)
	}
	return allRows
}

/**
查询单行
*/
func (Postgres Database) QueryRow() map[string]string {
	var queryStr = Postgres.fields + Postgres.whereStr + Postgres.orderBy + Postgres.limitNumber
	result, err := Postgres.conn.Query(queryStr)
	defer result.Close()
	Postgres.checkErr(err)
	Column, err := result.Columns()
	// 创建一个查询字段类型的slice的键值对
	values := make([]sql.RawBytes, len(Column))
	// 创建一个任意字段类型的slice的键值对
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		// 把values每个参数的地址存入scanArgs
		scanArgs[i] = &values[i]
	}

	for result.Next() {
		err = result.Scan(scanArgs...)
		Postgres.checkErr(err)
	}
	tempRow := make(map[string]string, len(Column))
	for i, col := range values {
		var key = Column[i]
		tempRow[key] = string(col)
	}
	return tempRow

}

/**
检查错误
*/
func (Postgres Database) checkErr(err error) {
	if err != nil {
		log.Fatal("错误：", err)
	}
}
