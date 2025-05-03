package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

const (
	chunkSize     = 100 * 1024 * 1024 // 分片大小 (1MB)，需要与服务器一致
	maxConcurrent = 100               // 最大并发上传数
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

// QuicClient 封装QUIC连接
type QuicClient struct {
	Conn quic.Connection
	URL  string
	sync.Mutex
	// 添加流控制
	StreamLimiter chan struct{}
}

// 从URL中提取主机和端口
func urlToHostWithPort(urlStr string) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}

	host := u.Host
	if u.Port() == "" {
		if u.Scheme == "https" || u.Scheme == "http3" {
			host = fmt.Sprintf("%s:443", u.Hostname())
		} else {
			host = fmt.Sprintf("%s:80", u.Hostname())
		}
	}

	return host, nil
}

// 创建QUIC客户端连接
func createQuicClient(serverURL string) (*QuicClient, error) {
	// 解析URL以获取主机和端口
	u, err := url.Parse(serverURL)
	if err != nil {
		return nil, fmt.Errorf("解析URL失败: %w", err)
	}

	// 提取主机名，并使用4434端口（服务器上用于直接流处理的端口）
	// 注意：原始URL保持不变用于HTTP请求
	host := fmt.Sprintf("%s:4434", u.Hostname())

	// 设置TLS配置
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // 警告：生产环境不要使用
		NextProtos:         []string{"h3"},
	}

	// QUIC配置
	quicConfig := &quic.Config{
		EnableDatagrams:            true,
		Allow0RTT:                  true,
		MaxIncomingStreams:         1000,              // 增加最大并发流数量
		MaxStreamReceiveWindow:     20 * 1024 * 1024,  // 增加到20MB的流接收窗口
		MaxConnectionReceiveWindow: 100 * 1024 * 1024, // 增加到100MB的连接接收窗口
		KeepAlivePeriod:            10 * time.Second,  // 保持连接活跃的间隔
		HandshakeIdleTimeout:       30 * time.Second,  // 握手超时
		MaxIdleTimeout:             120 * time.Second, // 最大空闲超时增加到120秒
		InitialStreamReceiveWindow: 512 * 1024,        // 初始流接收窗口
	}

	// 建立QUIC连接
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := quic.DialAddr(ctx, host, tlsConfig, quicConfig)
	if err != nil {
		return nil, fmt.Errorf("建立QUIC连接失败: %w", err)
	}

	// 创建限制并发流的通道，避免同时打开太多流
	// 这里设置为20，即最多同时有20个活跃流
	streamLimiter := make(chan struct{}, 50)

	return &QuicClient{
		Conn:          conn,
		URL:           u.Hostname(), // 只存储主机名，不含端口
		StreamLimiter: streamLimiter,
	}, nil
}

// 使用QUIC流发送JSON请求
func (qc *QuicClient) sendJSONViaStream(path string, data interface{}) (map[string]interface{}, error) {
	qc.Lock()
	defer qc.Unlock()

	// 将数据序列化为JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("JSON序列化失败: %w", err)
	}

	// 打开一个新的流
	stream, err := qc.Conn.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("打开QUIC流失败: %w", err)
	}
	defer stream.Close()

	// 构建HTTP请求头
	reqLine := fmt.Sprintf("POST %s HTTP/1.1\r\n", path)
	headers := fmt.Sprintf("Host: %s:4434\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n",
		qc.URL, len(jsonData))

	// 发送请求头和数据
	if _, err := stream.Write([]byte(reqLine + headers)); err != nil {
		return nil, fmt.Errorf("发送请求头失败: %w", err)
	}

	if _, err := stream.Write(jsonData); err != nil {
		return nil, fmt.Errorf("发送JSON数据失败: %w", err)
	}

	// 读取响应
	resp := make([]byte, 4096)
	n, err := stream.Read(resp)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	// 解析HTTP响应
	response := string(resp[:n])
	parts := bytes.Split(resp[:n], []byte("\r\n\r\n"))
	if len(parts) < 2 {
		return nil, fmt.Errorf("无效的HTTP响应: %s", response)
	}

	// 解析响应体JSON
	var respData map[string]interface{}
	if err := json.Unmarshal(parts[1], &respData); err != nil {
		return nil, fmt.Errorf("解析响应JSON失败: %w", err)
	}

	return respData, nil
}

// 使用QUIC流上传文件分片
func (qc *QuicClient) uploadChunkViaStream(fileID string, chunkNum int, data io.Reader, size int64) error {
	// 获取流许可
	qc.StreamLimiter <- struct{}{}
	defer func() { <-qc.StreamLimiter }()

	// 打开一个新的流，设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // 增加超时时间
	defer cancel()

	stream, err := qc.Conn.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("打开QUIC流失败: %w", err)
	}
	defer stream.Close()

	// 构建HTTP请求头
	path := fmt.Sprintf("/api/streamUploadChunk?fileId=%s&chunkNum=%d", fileID, chunkNum)
	reqLine := fmt.Sprintf("POST %s HTTP/1.1\r\n", path)
	headers := fmt.Sprintf("Host: %s:4434\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n",
		qc.URL, size)

	// 发送请求头
	if _, err := stream.Write([]byte(reqLine + headers)); err != nil {
		return fmt.Errorf("发送请求头失败: %w", err)
	}

	// 发送数据，使用带缓冲的写入
	buf := make([]byte, 32*1024) // 32KB缓冲区
	_, err = io.CopyBuffer(stream, data, buf)
	if err != nil {
		return fmt.Errorf("发送分片数据失败: %w", err)
	}

	// 设置读取超时
	deadline := time.Now().Add(30 * time.Second) // 增加读取超时
	if err := stream.SetReadDeadline(deadline); err != nil {
		return fmt.Errorf("设置读取超时失败: %w", err)
	}

	// 读取响应
	resp := make([]byte, 4096)
	n, err := stream.Read(resp)
	if err != nil && err != io.EOF {
		return fmt.Errorf("读取响应失败: %w", err)
	}

	// 解析HTTP响应
	response := string(resp[:n])
	if !strings.Contains(response, "200 OK") {
		return fmt.Errorf("服务器返回错误: %s", response)
	}

	// 检查JSON响应
	parts := bytes.Split(resp[:n], []byte("\r\n\r\n"))
	if len(parts) < 2 {
		return nil // 无响应体也视为成功
	}

	// 解析响应体JSON
	var respData map[string]interface{}
	if err := json.Unmarshal(parts[1], &respData); err != nil {
		return fmt.Errorf("解析响应JSON失败: %w", err)
	}

	// 检查上传是否成功
	if success, ok := respData["success"].(bool); !ok || !success {
		return fmt.Errorf("上传分片失败: %v", respData["message"])
	}

	return nil
}

func main() {
	// 命令行参数
	serverURL := flag.String("server", "https://localhost:4433", "服务器URL")
	filePath := flag.String("file", "1G", "要上传的文件路径")
	parallelism := flag.Int("parallel", maxConcurrent, "并行上传的分片数")
	resumeUpload := flag.Bool("resume", true, "是否启用断点续传")
	useStream := flag.Bool("stream", false, "是否使用QUIC流进行传输")
	flag.Parse()

	if *filePath == "" {
		log.Fatalf("请提供要上传的文件路径")
	}

	// 创建协程池管理并发上传
	pool, err := ants.NewPool(*parallelism)
	if err != nil {
		log.Fatalf("创建协程池失败: %v", err)
	}
	defer pool.Release()

	var err2 error
	if *useStream {
		// 使用QUIC流方式上传
		quicClient, err := createQuicClient(*serverURL)
		if err != nil {
			log.Fatalf("创建QUIC客户端失败: %v", err)
		}
		defer quicClient.Conn.CloseWithError(0, "")

		err2 = uploadFileWithStream(quicClient, *serverURL, *filePath, pool, *resumeUpload)
	} else {
		// 使用传统HTTP/3方式上传
		client, err := createHTTP3Client()
		if err != nil {
			log.Fatalf("创建HTTP/3客户端失败: %v", err)
		}

		err2 = uploadFile(client, *serverURL, *filePath, pool, *resumeUpload)
	}

	if err2 != nil {
		log.Fatalf("上传失败: %v", err2)
	}
}

// 创建HTTP/3客户端
func createHTTP3Client() (*http.Client, error) {
	// 设置TLS配置，允许自签名证书（仅用于开发测试）
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // 警告：生产环境不要使用这个选项
		NextProtos:         []string{"h3"},
		ClientSessionCache: tls.NewLRUClientSessionCache(100),
	}

	// QUIC配置
	quicConfig := &quic.Config{
		EnableDatagrams:            true,
		Allow0RTT:                  true,
		MaxIncomingStreams:         100,              // 增加最大并发流数量
		MaxStreamReceiveWindow:     10 * 1024 * 1024, // 增加流接收窗口到10MB
		MaxConnectionReceiveWindow: 15 * 1024 * 1024, // 连接接收窗口
		KeepAlivePeriod:            10 * time.Second, // 保持连接活跃
	}

	// 创建HTTP/3客户端
	roundTripper := &http3.Transport{
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

// 使用QUIC流上传文件
func uploadFileWithStream(quicClient *QuicClient, serverURL, filePath string, pool *ants.Pool, resumeUpload bool) error {
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
	resp, err := quicClient.sendJSONViaStream("/api/initUpload", initRequest)
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
	fmt.Printf("文件大小: %d 字节, 分片大小: %d 字节, 总分片数: %d\n",
		fileInfo.Size(), chunkSize, chunkCount)

	// 如果启用断点续传，获取已上传的分片列表
	uploadedChunks := make(map[int]bool)
	if resumeUpload {
		fmt.Println("检查断点续传状态...")
		// 创建一个临时HTTP客户端获取已上传的分片信息
		httpClient, err := createHTTP3Client()
		if err != nil {
			fmt.Printf("创建HTTP客户端失败: %v，将从头开始上传\n", err)
		} else {
			chunks, err := getUploadedChunks(httpClient, serverURL, fileID)
			if err != nil {
				fmt.Printf("获取已上传分片信息失败: %v，将从头开始上传\n", err)
			} else {
				for _, chunkNum := range chunks {
					uploadedChunks[chunkNum] = true
				}
				fmt.Printf("发现已上传的分片数: %d\n", len(uploadedChunks))
			}
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
	fmt.Println("开始上传文件分片，使用QUIC流传输...")

	var failedTasks sync.Map // 用于记录失败的任务

	for _, task := range tasks {
		// 提交到协程池
		taskCopy := task // 创建副本避免闭包问题
		err := pool.Submit(func() {
			defer wg.Done()
			err := uploadChunkWithStream(quicClient, filePath, taskCopy)
			progress.Update(err == nil)
			if err != nil {
				// 记录失败的任务
				failedTasks.Store(taskCopy.ChunkNum, taskCopy)
			}
		})

		if err != nil {
			return fmt.Errorf("提交上传任务失败: %w", err)
		}
	}

	// 等待所有分片上传完成
	wg.Wait()
	progress.Finish()

	// 处理失败的分片
	var failedCount int
	failedTasks.Range(func(key, value interface{}) bool {
		failedCount++
		return true
	})

	// 如果有失败的分片，返回错误但同时提供详细信息
	if failedCount > 0 {
		fmt.Printf("有 %d 个分片上传失败，需要重试。\n", failedCount)
		fmt.Printf("失败的分片编号: ")
		failedTasks.Range(func(key, _ interface{}) bool {
			fmt.Printf("%d ", key.(int))
			return true
		})
		fmt.Println()
		return fmt.Errorf("有 %d 个分片上传失败，请使用 --resume 选项重试", failedCount)
	}

	// 完成上传
	fmt.Println("通知服务器合并文件...")
	completeRequest := map[string]interface{}{
		"fileId": fileID,
	}

	_, err = quicClient.sendJSONViaStream("/api/completeUpload", completeRequest)
	if err != nil {
		return fmt.Errorf("请求文件合并失败: %w", err)
	}

	// 轮询文件状态，直到合并完成
	fmt.Println("等待服务器合并文件...")
	// 创建一个临时HTTP客户端检查文件状态
	httpClient, err := createHTTP3Client()
	if err != nil {
		return fmt.Errorf("创建HTTP客户端失败: %v", err)
	}

	for {
		statusResp, err := httpClient.Get(serverURL + "/api/fileStatus?fileId=" + fileID)
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

	return nil
}

// 使用QUIC流上传单个分片
func uploadChunkWithStream(quicClient *QuicClient, filePath string, task ChunkTask) error {
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

	// 创建有限的读取器，只读取当前分片大小的数据
	limitReader := io.LimitReader(file, task.Size)

	// 允许重试3次
	for retry := 0; retry < 3; retry++ {
		err := quicClient.uploadChunkViaStream(task.FileID, task.ChunkNum, limitReader, task.Size)
		if err == nil {
			return nil
		}

		// 重新定位文件指针
		file.Seek(task.Offset, 0)
		limitReader = io.LimitReader(file, task.Size)

		log.Printf("分片 %d 上传失败(第%d次重试): %v", task.ChunkNum, retry+1, err)
		// 短暂休眠后重试
		time.Sleep(time.Millisecond * 500)
	}

	return fmt.Errorf("上传分片 %d 失败，已重试3次", task.ChunkNum)
}
