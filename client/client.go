package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	serverURL  = "https://localhost:8443"
	apiBaseURL = serverURL + "/api"
	chunkSize  = 100 * 1024 * 1024 // 100MB
)

// 用户认证令牌
var token string

// 用户登录请求
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// 用户注册请求
type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Email    string `json:"email"`
}

// 文件信息结构体
type FileInfo struct {
	FileID      string    `json:"fileId"`      // 文件唯一标识符
	FileName    string    `json:"fileName"`    // 文件名
	FileSize    int64     `json:"fileSize"`    // 文件大小
	FileHash    string    `json:"fileHash"`    // 文件MD5哈希值
	ChunkCount  int       `json:"chunkCount"`  // 分片总数
	CreatedAt   time.Time `json:"createdAt"`   // 创建时间
	IsCompleted bool      `json:"isCompleted"` // 是否上传完成
}

// 初始化上传响应
type InitUploadResponse struct {
	Success       bool     `json:"success"`
	Message       string   `json:"message"`
	InstantUpload bool     `json:"instantUpload"`
	FileInfo      FileInfo `json:"fileInfo"`
}

// 上传分片响应
type UploadChunkResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	ChunkNum int    `json:"chunkNum"`
}

// 主函数
func main() {
	// 禁用证书验证（仅用于开发测试）
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	// 打印欢迎信息
	fmt.Println("=== QUIC文件上传客户端 ===")

	// 主菜单循环
	for {
		printMainMenu()
		option := readInput("请选择操作")

		switch option {
		case "1":
			register()
		case "2":
			login()
		case "3":
			if token == "" {
				fmt.Println("请先登录！")
				continue
			}
			uploadFile()
		case "4":
			if token == "" {
				fmt.Println("请先登录！")
				continue
			}
			listFiles()
		case "5":
			fmt.Println("退出程序...")
			return
		default:
			fmt.Println("无效选项，请重试")
		}
	}
}

// 打印主菜单
func printMainMenu() {
	fmt.Println("\n===== 主菜单 =====")
	fmt.Println("1. 注册")
	fmt.Println("2. 登录")
	fmt.Println("3. 上传文件")
	fmt.Println("4. 查看文件列表")
	fmt.Println("5. 退出")
	fmt.Println("=================")
}

// 用户注册
func register() {
	fmt.Println("\n==== 用户注册 ====")
	username := readInput("请输入用户名")
	password := readInput("请输入密码")
	email := readInput("请输入邮箱")

	// 创建注册请求
	registerReq := RegisterRequest{
		Username: username,
		Password: password,
		Email:    email,
	}

	// 发送注册请求
	jsonData, err := json.Marshal(registerReq)
	if err != nil {
		fmt.Printf("注册失败: %v\n", err)
		return
	}

	resp, err := http.Post(apiBaseURL+"/user/register", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("注册请求失败: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("解析响应失败: %v\n", err)
		return
	}

	// 检查注册结果
	if success, ok := result["success"].(bool); ok && success {
		fmt.Println("注册成功！请登录")

		// 自动设置token
		if data, ok := result["data"].(map[string]interface{}); ok {
			if tokenValue, ok := data["token"].(string); ok {
				token = tokenValue
				fmt.Println("已自动登录")
			}
		}
	} else {
		message := "未知错误"
		if msg, ok := result["message"].(string); ok {
			message = msg
		}
		fmt.Printf("注册失败: %s\n", message)
	}
}

// 用户登录
func login() {
	fmt.Println("\n==== 用户登录 ====")
	username := readInput("请输入用户名")
	password := readInput("请输入密码")

	// 创建登录请求
	loginReq := LoginRequest{
		Username: username,
		Password: password,
	}

	// 发送登录请求
	jsonData, err := json.Marshal(loginReq)
	if err != nil {
		fmt.Printf("登录失败: %v\n", err)
		return
	}

	resp, err := http.Post(apiBaseURL+"/user/login", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("登录请求失败: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("解析响应失败: %v\n", err)
		return
	}

	// 检查登录结果
	if success, ok := result["success"].(bool); ok && success {
		if data, ok := result["data"].(map[string]interface{}); ok {
			if tokenValue, ok := data["token"].(string); ok {
				token = tokenValue
				fmt.Println("登录成功！")
			}
		}
	} else {
		message := "未知错误"
		if msg, ok := result["message"].(string); ok {
			message = msg
		}
		fmt.Printf("登录失败: %s\n", message)
	}
}

// 上传文件
func uploadFile() {
	fmt.Println("\n==== 文件上传 ====")
	filePath := readInput("请输入文件绝对路径")

	// 检查文件是否存在
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("文件不存在: %s\n", filePath)
		return
	}

	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("打开文件失败: %v\n", err)
		return
	}
	defer file.Close()

	// 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Printf("获取文件信息失败: %v\n", err)
		return
	}

	fileName := filepath.Base(filePath)
	fileSize := fileInfo.Size()
	totalChunks := int(math.Ceil(float64(fileSize) / float64(chunkSize)))

	// 计算文件MD5哈希值
	var fileHash string
	if fileSize > 1024*1024*1024 { // 大于1GB的文件
		fmt.Printf("文件大小超过1GB (%.2f GB)，计算快速哈希以提高效率...\n", float64(fileSize)/(1024*1024*1024))
		// 对大文件计算部分哈希，避免耗时太长
		fileHash, err = calculatePartialHash(file, fileSize)
	} else {
		fmt.Printf("计算文件完整哈希中，请耐心等待...\n")
		fileHash, err = calculateFullHash(file)
	}

	if err != nil {
		fmt.Printf("计算文件哈希失败: %v\n", err)
		return
	}

	file.Seek(0, 0) // 重置文件指针到开头

	// 生成文件ID
	fileID := uuid.New().String() // 生成文件唯一标识

	fmt.Printf("准备上传文件: %s (大小: %.2f MB, 分片数: %d, 哈希: %s, ID: %s)\n",
		fileName, float64(fileSize)/(1024*1024), totalChunks, fileHash, fileID)

	// 创建初始化上传请求
	initUploadReq := map[string]interface{}{
		"fileId":     fileID,
		"fileName":   fileName,
		"fileSize":   fileSize,
		"fileHash":   fileHash,
		"chunkCount": totalChunks,
	}

	// 发送初始化上传请求
	jsonData, err := json.Marshal(initUploadReq)
	if err != nil {
		fmt.Printf("创建上传请求失败: %v\n", err)
		return
	}

	fmt.Printf("发送初始化请求: %s\n", string(jsonData))
	req, err := http.NewRequest("POST", apiBaseURL+"/initUpload", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("创建请求失败: %v\n", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{
		Timeout: 30 * time.Second, // 设置30秒超时
	}

	fmt.Println("发送初始化上传请求...")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("发送初始化请求失败: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// 读取响应内容
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("读取响应失败: %v\n", err)
		return
	}

	// 记录完整响应内容用于调试
	fmt.Printf("接收到初始化响应: HTTP状态码=%d\n响应内容: %s\n", resp.StatusCode, string(responseBody))
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("服务器返回错误状态码: %d\n%s\n", resp.StatusCode, string(responseBody))
		return
	}

	var initResult map[string]interface{}
	if err := json.Unmarshal(responseBody, &initResult); err != nil {
		fmt.Printf("解析响应失败: %v\n原始响应: %s\n", err, string(responseBody))
		return
	}

	// 检查请求是否成功
	success, ok := initResult["success"].(bool)
	if !ok || !success {
		message := "未知错误"
		if msg, ok := initResult["message"].(string); ok {
			message = msg
		}
		fmt.Printf("初始化上传失败: %s\n", message)
		return
	}

	// 获取返回的文件信息
	fileInfoMap, ok := initResult["fileInfo"].(map[string]interface{})
	if !ok {
		fmt.Println("响应中缺少文件信息")
		return
	}

	// 使用服务器返回的文件ID，而不是客户端生成的
	if serverFileID, ok := fileInfoMap["fileId"].(string); ok && serverFileID != "" {
		fileID = serverFileID
		fmt.Printf("使用服务器返回的文件ID: %s\n", fileID)
	}

	// 检查是否可以秒传
	if instantUpload, ok := initResult["instantUpload"].(bool); ok && instantUpload {
		fmt.Println("文件已存在，秒传成功！")
		return
	}

	// 获取已上传的分片
	uploadedChunks := make(map[int]bool)
	getUploadedChunks(fileID, uploadedChunks)

	// 上传文件分片
	successful := true
	for i := 0; i < totalChunks; i++ {
		// 如果分片已上传，跳过
		if uploadedChunks[i] {
			fmt.Printf("分片 %d/%d 已上传，跳过\n", i+1, totalChunks)
			continue
		}

		// 计算分片大小
		chunkStart := int64(i) * chunkSize
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > fileSize {
			chunkEnd = fileSize
		}
		currentChunkSize := chunkEnd - chunkStart

		// 创建分片数据
		chunk := make([]byte, currentChunkSize)
		file.Seek(chunkStart, 0)
		_, err := io.ReadFull(file, chunk)
		if err != nil {
			fmt.Printf("读取分片 %d 失败: %v\n", i, err)
			successful = false
			break
		}

		// 上传分片
		success := uploadChunk(fileID, i, chunk)
		if !success {
			fmt.Printf("上传分片 %d 失败，中止上传\n", i)
			successful = false
			break
		}

		fmt.Printf("上传分片 %d/%d 成功 (%.2f%%)\n",
			i+1, totalChunks, float64(i+1)/float64(totalChunks)*100)
	}

	if !successful {
		fmt.Println("文件上传失败，请稍后重试")
		return
	}

	// 完成上传
	completeUploadReq := map[string]interface{}{
		"fileId": fileID,
	}

	jsonData, err = json.Marshal(completeUploadReq)
	if err != nil {
		fmt.Printf("创建完成请求失败: %v\n", err)
		return
	}

	fmt.Printf("发送完成上传请求: %s\n", string(jsonData))
	req, err = http.NewRequest("POST", apiBaseURL+"/completeUpload", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("创建请求失败: %v\n", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	fmt.Println("请求合并文件...")
	resp, err = client.Do(req)
	if err != nil {
		fmt.Printf("发送完成请求失败: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// 读取完整响应
	responseBody, err = io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("读取响应失败: %v\n", err)
		return
	}

	// 检查HTTP状态码
	fmt.Printf("收到完成上传响应: HTTP状态码=%d\n", resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("服务器返回错误状态码: %d\n响应内容: %s\n", resp.StatusCode, string(responseBody))
		return
	}

	// 解析响应
	var completeResult map[string]interface{}
	if err := json.Unmarshal(responseBody, &completeResult); err != nil {
		fmt.Printf("解析响应失败: %v\n原始响应: %s\n", err, string(responseBody))
		return
	}

	if success, ok := completeResult["success"].(bool); ok && success {
		fmt.Println("文件上传完成！服务器正在处理合并任务，请稍后通过文件列表查看状态")
	} else {
		message := "未知错误"
		if msg, ok := completeResult["message"].(string); ok {
			message = msg
		}
		fmt.Printf("完成上传失败: %s\n", message)
		fmt.Printf("完整响应内容: %s\n", string(responseBody))
	}
}

// 计算完整文件的MD5哈希
func calculateFullHash(file *os.File) (string, error) {
	startTime := time.Now()
	file.Seek(0, 0) // 重置文件指针到开头

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("计算哈希失败: %w", err)
	}

	hashValue := hex.EncodeToString(hash.Sum(nil))
	duration := time.Since(startTime)

	fmt.Printf("完整哈希计算完成，耗时: %.2f秒, MD5=%s\n", duration.Seconds(), hashValue)
	return hashValue, nil
}

// 对大文件计算部分哈希
func calculatePartialHash(file *os.File, fileSize int64) (string, error) {
	startTime := time.Now()
	file.Seek(0, 0) // 重置文件指针到开头

	// 选择文件的头部、中部和尾部进行采样
	sampleSize := int64(10 * 1024 * 1024) // 每部分采样10MB
	if sampleSize > fileSize/3 {
		sampleSize = fileSize / 3
	}

	// 创建一个复合哈希
	hash := md5.New()

	// 读取文件头部
	headBuffer := make([]byte, sampleSize)
	if _, err := io.ReadFull(file, headBuffer); err != nil {
		return "", fmt.Errorf("读取文件头部失败: %w", err)
	}
	hash.Write(headBuffer)

	// 读取文件中部
	middleOffset := (fileSize - sampleSize) / 2
	file.Seek(middleOffset, 0)
	middleBuffer := make([]byte, sampleSize)
	if _, err := io.ReadFull(file, middleBuffer); err != nil {
		return "", fmt.Errorf("读取文件中部失败: %w", err)
	}
	hash.Write(middleBuffer)

	// 读取文件尾部
	tailOffset := fileSize - sampleSize
	file.Seek(tailOffset, 0)
	tailBuffer := make([]byte, sampleSize)
	if _, err := io.ReadFull(file, tailBuffer); err != nil {
		return "", fmt.Errorf("读取文件尾部失败: %w", err)
	}
	hash.Write(tailBuffer)

	// 计算文件大小的hash并加入
	sizeBytes := []byte(fmt.Sprintf("%d", fileSize))
	hash.Write(sizeBytes)

	hashValue := hex.EncodeToString(hash.Sum(nil))
	duration := time.Since(startTime)

	fmt.Printf("对大文件计算快速哈希完成，采样大小:%.2fMB，耗时:%.2f秒, 快速哈希=%s\n",
		float64(sampleSize*3)/(1024*1024), duration.Seconds(), hashValue)
	return hashValue, nil
}

// 获取已上传的分片
func getUploadedChunks(fileID string, uploadedChunks map[int]bool) {
	req, err := http.NewRequest("GET", apiBaseURL+"/getUploadedChunks?fileId="+fileID, nil)
	if err != nil {
		fmt.Printf("创建请求失败: %v\n", err)
		return
	}

	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("获取已上传分片失败: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("解析响应失败: %v\n", err)
		return
	}

	if chunks, ok := result["chunks"].([]interface{}); ok {
		fmt.Printf("已上传 %d 个分片\n", len(chunks))
		for _, chunk := range chunks {
			if chunkNum, ok := chunk.(float64); ok {
				uploadedChunks[int(chunkNum)] = true
			}
		}
	}
}

// 上传单个分片
func uploadChunk(fileID string, chunkNum int, chunkData []byte) bool {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)

	// 添加fileID字段
	if err := w.WriteField("fileId", fileID); err != nil {
		fmt.Printf("写入fileId失败: %v\n", err)
		return false
	}

	// 添加chunkNum字段
	if err := w.WriteField("chunkNum", strconv.Itoa(chunkNum)); err != nil {
		fmt.Printf("写入chunkNum失败: %v\n", err)
		return false
	}

	// 添加文件内容
	fw, err := w.CreateFormFile("chunk", fmt.Sprintf("chunk-%d", chunkNum))
	if err != nil {
		fmt.Printf("创建form文件失败: %v\n", err)
		return false
	}
	if _, err := fw.Write(chunkData); err != nil {
		fmt.Printf("写入chunk数据失败: %v\n", err)
		return false
	}

	w.Close()

	req, err := http.NewRequest("POST", apiBaseURL+"/uploadChunk", &b)
	if err != nil {
		fmt.Printf("创建请求失败: %v\n", err)
		return false
	}

	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("上传分片请求失败: %v\n", err)
		return false
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("解析响应失败: %v\n", err)
		return false
	}

	return result["success"] == true
}

// 获取文件列表
func listFiles() {
	fmt.Println("\n==== 我的文件 ====")

	req, err := http.NewRequest("GET", apiBaseURL+"/userFiles", nil)
	if err != nil {
		fmt.Printf("创建请求失败: %v\n", err)
		return
	}

	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("获取文件列表失败: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Printf("解析响应失败: %v\n", err)
		return
	}

	if !result["success"].(bool) {
		fmt.Printf("获取文件列表失败: %s\n", result["message"])
		return
	}

	if files, ok := result["data"].([]interface{}); ok {
		if len(files) == 0 {
			fmt.Println("没有上传的文件")
			return
		}

		fmt.Println("ID\t文件名\t大小\t状态\t创建时间")
		fmt.Println("-------------------------------------------------------------------")

		for _, file := range files {
			fileInfo := file.(map[string]interface{})

			fileSize := fileInfo["fileSize"].(float64) / (1024 * 1024) // 转MB
			status := "已完成"
			if !fileInfo["isCompleted"].(bool) {
				status = "处理中"
			}

			fmt.Printf("%s\t%s\t%.2fMB\t%s\t%s\n",
				fileInfo["fileId"],
				fileInfo["fileName"],
				fileSize,
				status,
				fileInfo["createdAt"],
			)
		}
	} else {
		fmt.Println("没有文件或格式错误")
	}
}

// 从控制台读取输入
func readInput(prompt string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt + ": ")
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}
