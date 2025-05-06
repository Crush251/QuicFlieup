package main

import (
	"QuicFlieup/server"
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	// 解析命令行参数
	flag.Parse()

	// 创建服务器实例
	srv := server.NewServer()

	// 启动服务器
	fmt.Println("QUIC文件上传服务器正在启动...")
	if err := srv.Start(); err != nil {
		log.Fatalf("服务器启动失败: %v", err)
		os.Exit(1)
	}
}
