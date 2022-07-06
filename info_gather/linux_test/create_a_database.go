package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dsn := "root" + ":" + "" + "@tcp(127.0.0.1:3306)/" + "test_database"
	db, err := sql.Open("mysql", dsn) //defer db.Close() // 注意这行代码要写在上面err判断的下面
	if err != nil {
		fmt.Println("mysql建立链接出错:", err)
		return
	}
	err = db.Ping()
	if err != nil {
		fmt.Println("mysql建立链接出错:")
		panic(err)
	}
	fmt.Println("mysql连接成功!")
	sql_l := "CREATE TABLE " + "new_table" + "(id int PRIMARY KEY AUTO_INCREMENT, context Blob)" + "ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;"
	db.Exec(sql_l)
}
