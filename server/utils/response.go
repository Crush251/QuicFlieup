package utils

import (
	"encoding/json"
	"net/http"
)

// JSONResponse 发送JSON格式的HTTP响应
func JSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	// 设置响应头
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	// 序列化数据为JSON
	resp, err := json.Marshal(data)
	if err != nil {
		// 如果序列化失败，返回错误信息
		http.Error(w, `{"success":false,"message":"响应序列化失败"}`, http.StatusInternalServerError)
		return
	}

	// 写入响应体
	w.Write(resp)
}
