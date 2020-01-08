package dbutils

import (
	"fmt"
	"strings"
)

/**
插入方法
*/
func (Postgres Database) InsertByMySQL(tableName string, data map[string]interface{}) int64 {
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
