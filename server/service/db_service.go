package service

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// DBService 数据库服务
type DBService struct {
	DB *sql.DB
}

// NewDBService 创建新的数据库连接服务
func NewDBService(driverName, dataSourceName string) (*DBService, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	// 检查连接
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	// 设置连接池配置
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)

	return &DBService{DB: db}, nil
}

// Close 关闭数据库连接
func (dbs *DBService) Close() error {
	if dbs.DB != nil {
		return dbs.DB.Close()
	}
	return nil
}

// CreateMySQLDataSource 创建MySQL数据源字符串
func CreateMySQLDataSource(username, password, host string, port int, dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true",
		username, password, host, port, dbName)
}
