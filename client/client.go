package main

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	chunkSize     = 1 * 1024 * 1024 // 分片大小 (4MB)，需要与服务器一致
	maxConcurrent = 5               // 最大并发上传数
)

// 文件信息结构
type FileInfo struct {
	FileID      string    `json:"fileId"`      // 文件唯一标识符
	FileName    string    `json:"fileName"`    // 文件名
	FileSize    int64     `json:"fileSize"`    // 文件大小
	FileHash    string    `json:"fileHash"`    // 文件MD5哈希值
	ChunkCount  int       `json:"chunkCount"`  // 分片总数
	CreatedAt   time.Time `json:"createdAt"`   // 创建时间
	IsCompleted bool      `json:"isCompleted"` // 是否上传完成
}

// 分片上传任务
type ChunkTask struct {
	FileID   string
	ChunkNum int
	FilePath string
	Offset   int64
	Size     int64
}

// 上传进度跟踪
type ProgressTracker struct {
	sync.Mutex
	TotalChunks    int
	UploadedChunks int
	FailedChunks   int
	StartTime      time.Time
}

// 更新和显示进度
func (p *ProgressTracker) Update(success bool) {
	p.Lock()
	defer p.Unlock()

	if success {
		p.UploadedChunks++
	} else {
		p.FailedChunks++
	}

	progress := float64(p.UploadedChunks) / float64(p.TotalChunks) * 100
	elapsedTime := time.Since(p.StartTime).Seconds()
	speed := float64(p.UploadedChunks) * chunkSize / (1024 * 1024) / elapsedTime

	fmt.Printf("\r上传进度: %.2f%% (%d/%d) 速度: %.2f MB/s",
		progress, p.UploadedChunks, p.TotalChunks, speed)
}

// 完成显示
func (p *ProgressTracker) Finish() {
	p.Lock()
	defer p.Unlock()

	elapsedTime := time.Since(p.StartTime).Seconds()
	fmt.Printf("\n上传完成，总耗时: %.2f秒，平均速度: %.2f MB/s\n",
		elapsedTime, float64(p.UploadedChunks)*chunkSize/(1024*1024)/elapsedTime)
}

func main() {
	// 命令行参数
	serverURL := flag.String("server", "https://localhost:4433", "服务器URL")
	filePath := flag.String("file", "2.txt", "要上传的文件路径")
	parallelism := flag.Int("parallel", maxConcurrent, "并行上传的分片数")
	resumeUpload := flag.Bool("resume", true, "是否启用断点续传")
	flag.Parse()

	if *filePath == "" {
		log.Fatalf("请提供要上传的文件路径")
	}

	// 创建HTTP/3客户端
	client, err := createHTTP3Client()
	if err != nil {
		log.Fatalf("创建HTTP/3客户端失败: %v", err)
	}

	// 使用ants协程池管理并发上传
	pool, err := ants.NewPool(*parallelism)
	if err != nil {
		log.Fatalf("创建协程池失败: %v", err)
	}
	defer pool.Release()

	// 上传文件
	err = uploadFile(client, *serverURL, *filePath, pool, *resumeUpload)
	if err != nil {
		log.Fatalf("上传失败: %v", err)
	}
	// 添加协议检测
	resp, err := client.Get(*serverURL + "/api/ping")
	if err == nil {
		log.Printf("实际使用协议: %s", resp.Proto) // 将输出 "HTTP/3.0"
		log.Printf("传输层协议  : UDP")
	}
}

// 创建HTTP/3客户端
func createHTTP3Client() (*http.Client, error) {
	// 设置TLS配置，允许自签名证书（仅用于开发测试）
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // 警告：生产环境不要使用这个选项
		NextProtos:         []string{"h3"},
	}

	// QUIC配置
	quicConfig := &quic.Config{
		EnableDatagrams:    true,
		Allow0RTT:          true,
		MaxIncomingStreams: 100, // 增加最大并发流数量
	}

	// 创建HTTP/3客户端
	roundTripper := &http3.RoundTripper{
		TLSClientConfig:    tlsConfig,
		QUICConfig:         quicConfig,
		DisableCompression: true, // 关闭压缩以提升流性能
	}

	return &http.Client{
		Transport: roundTripper,
	}, nil
}

// 计算文件MD5哈希值
func calculateFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("计算哈希失败: %w", err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// 上传文件主流程
func uploadFile(client *http.Client, serverURL, filePath string, pool *ants.Pool, resumeUpload bool) error {
	// 获取文件信息
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("获取文件信息失败: %w", err)
	}

	// 计算文件哈希值
	fmt.Println("正在计算文件哈希值...")
	fileHash, err := calculateFileHash(filePath)
	if err != nil {
		return err
	}

	// 准备文件信息
	fileName := filepath.Base(filePath)
	fileID := uuid.New().String()

	// 初始化上传请求
	fmt.Println("初始化上传...")
	initRequest := map[string]interface{}{
		"fileId":   fileID,
		"fileName": fileName,
		"fileSize": fileInfo.Size(),
		"fileHash": fileHash,
	}

	// 发送初始化请求
	resp, err := sendJSONRequest(client, serverURL+"/api/initUpload", initRequest)
	if err != nil {
		return fmt.Errorf("初始化上传失败: %w", err)
	}

	// 检查是否可以秒传
	if resp["instantUpload"] == true {
		fmt.Println("文件已存在，秒传成功")
		return nil
	}

	// 计算分片数量
	chunkCount := int((fileInfo.Size() + chunkSize - 1) / chunkSize)

	// 如果启用断点续传，获取已上传的分片列表
	uploadedChunks := make(map[int]bool)
	if resumeUpload {
		fmt.Println("检查断点续传状态...")
		chunks, err := getUploadedChunks(client, serverURL, fileID)
		if err != nil {
			fmt.Printf("获取已上传分片信息失败: %v，将从头开始上传\n", err)
		} else {
			for _, chunkNum := range chunks {
				uploadedChunks[chunkNum] = true
			}
			fmt.Printf("发现已上传的分片数: %d\n", len(uploadedChunks))
		}
	}

	// 准备上传任务
	tasks := prepareTasks(filePath, fileID, fileInfo.Size(), uploadedChunks)

	// 创建进度跟踪器
	progress := &ProgressTracker{
		TotalChunks: chunkCount,
		StartTime:   time.Now(),
	}

	// 已上传的分片不计入总数
	progress.UploadedChunks = len(uploadedChunks)

	// 创建等待组来同步所有上传任务
	var wg sync.WaitGroup
	wg.Add(len(tasks))

	// 提交所有分片上传任务
	fmt.Println("开始上传文件分片...")

	for _, task := range tasks {
		// 提交到协程池
		taskCopy := task // 创建副本避免闭包问题
		err := pool.Submit(func() {
			defer wg.Done()
			err := uploadChunk(client, serverURL, filePath, taskCopy)
			progress.Update(err == nil)
		})

		if err != nil {
			return fmt.Errorf("提交上传任务失败: %w", err)
		}
	}

	// 等待所有分片上传完成
	wg.Wait()
	progress.Finish()

	// 如果有失败的分片，提示重试
	if progress.FailedChunks > 0 {
		return fmt.Errorf("有 %d 个分片上传失败，请尝试使用 --resume 选项重试", progress.FailedChunks)
	}

	// 完成上传
	fmt.Println("通知服务器合并文件...")
	completeRequest := map[string]interface{}{
		"fileId": fileID,
	}

	_, err = sendJSONRequest(client, serverURL+"/api/completeUpload", completeRequest)
	if err != nil {
		return fmt.Errorf("请求文件合并失败: %w", err)
	}

	// 轮询文件状态，直到合并完成
	fmt.Println("等待服务器合并文件...")
	for {
		statusResp, err := client.Get(serverURL + "/api/fileStatus?fileId=" + fileID)
		if err != nil {
			return fmt.Errorf("查询文件状态失败: %w", err)
		}

		var statusData map[string]interface{}
		err = json.NewDecoder(statusResp.Body).Decode(&statusData)
		statusResp.Body.Close()
		if err != nil {
			return fmt.Errorf("解析文件状态响应失败: %w", err)
		}

		fileInfoData, ok := statusData["fileInfo"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("响应中缺少文件信息")
		}

		isCompleted, ok := fileInfoData["isCompleted"].(bool)
		if ok && isCompleted {
			fmt.Println("文件上传和合并完成！")
			break
		}

		// 每秒检查一次状态
		time.Sleep(1 * time.Second)
	}

	//return tasks
	return nil
}

// 获取已上传的分片列表
func getUploadedChunks(client *http.Client, serverURL, fileID string) ([]int, error) {
	resp, err := client.Get(fmt.Sprintf("%s/api/getUploadedChunks?fileId=%s", serverURL, fileID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var respData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return nil, err
	}

	if success, ok := respData["success"].(bool); !ok || !success {
		return nil, fmt.Errorf("获取已上传分片失败: %v", respData["message"])
	}

	chunksData, ok := respData["uploadedChunks"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("响应中缺少已上传分片信息")
	}

	chunks := make([]int, len(chunksData))
	for i, chunk := range chunksData {
		if chunkNum, ok := chunk.(float64); ok {
			chunks[i] = int(chunkNum)
		}
	}

	return chunks, nil
}

// 修改3：在roundTripper中获取底层QUIC连接（新增帮助函数）
func getQUICRoundTripper(client *http.Client) *http3.RoundTripper {
	return client.Transport.(*http3.RoundTripper)
}

// 上传单个分片
func uploadChunk(client *http.Client, serverURL, filePath string, task ChunkTask) error {
	// 打开文件
	file, err := os.Open(task.FilePath)
	if err != nil {
		return fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	// 移动到对应的分片位置
	_, err = file.Seek(task.Offset, 0)
	if err != nil {
		return fmt.Errorf("文件定位失败: %w", err)
	}

	// 准备请求URL
	url := fmt.Sprintf("%s/api/uploadChunk?fileId=%s&chunkNum=%d",
		serverURL, task.FileID, task.ChunkNum)

	// 创建有限的读取器，只读取当前分片大小的数据
	limitReader := io.LimitReader(file, task.Size)

	// 创建HTTP请求
	req, err := http.NewRequest("POST", url, limitReader)
	if err != nil {
		return fmt.Errorf("创建上传请求失败: %w", err)
	}

	// 设置请求头
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", task.Size))

	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("上传分片请求失败: %w", err)
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("服务器返回错误: %s, 状态码: %d", string(body), resp.StatusCode)
	}

	// 解析响应
	var respData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return fmt.Errorf("解析响应失败: %w", err)
	}

	// 检查上传是否成功
	if success, ok := respData["success"].(bool); !ok || !success {
		return fmt.Errorf("上传分片失败: %v", respData["message"])
	}

	return nil
}

// 发送JSON请求
func sendJSONRequest(client *http.Client, url string, data interface{}) (map[string]interface{}, error) {
	// 将数据序列化为JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("序列化JSON失败: %w", err)
	}

	// 创建请求
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("服务器返回错误: %s, 状态码: %d", string(body), resp.StatusCode)
	}

	// 解析响应
	var respData map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	return respData, nil
}

// 准备上传任务
func prepareTasks(filePath, fileID string, fileSize int64, uploadedChunks map[int]bool) []ChunkTask {
	var tasks []ChunkTask

	// 计算分片任务
	chunkCount := int((fileSize + chunkSize - 1) / chunkSize)

	for i := 0; i < chunkCount; i++ {
		// 如果分片已上传，跳过
		if uploadedChunks[i] {
			continue
		}

		offset := int64(i) * chunkSize
		size := chunkSize
		if offset+int64(size) > fileSize {
			size = int(fileSize - offset)
		}

		tasks = append(tasks, ChunkTask{
			FileID:   fileID,
			ChunkNum: i,
			FilePath: filePath,
			Offset:   offset,
			Size:     int64(size),
		})
	}
	return tasks
}
