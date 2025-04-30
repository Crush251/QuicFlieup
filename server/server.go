package main

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/logging"
	"github.com/quic-go/quic-go/qlog"
	"io"
	"log"
	"net/http" // 新增标准http包
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	uploadDir      = "./uploads"     // 上传文件的保存目录
	tempDir        = "./temp"        // 临时文件目录，用于保存分片
	chunkSize      = 1 * 1024 * 1024 // 分片大小 (4MB)
	workerPoolSize = 100             // 工作协程池大小
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

// 分片上传请求
type ChunkUploadRequest struct {
	FileID    string `json:"fileId"`    // 文件ID
	ChunkNum  int    `json:"chunkNum"`  // 分片序号
	TotalSize int64  `json:"totalSize"` // 总大小
}

// 文件信息存储，实际项目中应使用数据库
var (
	fileInfoMap     = make(map[string]*FileInfo) // 存储文件信息的映射表
	fileInfoMapLock sync.RWMutex                 // 读写锁保护映射表
	fileHashMap     = make(map[string]string)    // 哈希值到文件ID的映射，用于秒传
	fileHashLock    sync.RWMutex                 // 读写锁保护哈希映射表
)

func main() {
	// 确保上传和临时目录存在
	ensureDirectories()

	// 创建工作协程池
	pool, err := ants.NewPool(workerPoolSize)
	if err != nil {
		log.Fatalf("创建协程池失败: %v", err)
	}
	defer pool.Release()

	// 设置QUIC参数
	quicConfig := &quic.Config{
		EnableDatagrams: true,
		Allow0RTT:       true,
		// 更新QLOG支持
		Tracer: func(ctx context.Context, p logging.Perspective, ci quic.ConnectionID) *logging.ConnectionTracer {
			filename := fmt.Sprintf("./qlog-%s-%x.qlog", p, ci.Bytes())
			f, err := os.Create(filename)
			if err != nil {
				log.Printf("无法创建qlog文件: %s", err)
				return nil
			}
			return qlog.NewConnectionTracer(f, p, ci)
		},
	}

	// 加载证书（需要生成有效的证书）
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatalf("加载证书失败: %v", err)
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h3"}, // HTTP/3协议
	}

	// 创建HTTP/3服务器
	server := http3.Server{
		Addr:       ":4433",
		TLSConfig:  tlsConfig,
		QUICConfig: quicConfig,
		Handler:    setupHandlers(pool),
	}

	log.Println("HTTP/3服务器启动在 https://localhost:4433")
	err = server.ListenAndServe()
	if err != nil {
		log.Fatalf("服务器启动失败: %v", err)
	}
}

// 确保必要的目录存在
func ensureDirectories() {
	dirs := []string{uploadDir, tempDir}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("创建目录失败 %s: %v", dir, err)
		}
	}
}

// 设置HTTP路由处理器
func setupHandlers(pool *ants.Pool) http.Handler {
	mux := http.NewServeMux()

	// 初始化上传请求处理
	mux.HandleFunc("/api/initUpload", func(w http.ResponseWriter, r *http.Request) {
		var fileInfo FileInfo
		if err := json.NewDecoder(r.Body).Decode(&fileInfo); err != nil {
			http3Error(w, "无效的请求数据", 400)
			return
		}

		// 检查是否可以秒传
		fileHashLock.RLock()
		existingFileID, exists := fileHashMap[fileInfo.FileHash]
		fileHashLock.RUnlock()

		if exists {
			// 文件已存在，可以秒传
			fileInfoMapLock.RLock()
			existingFile := fileInfoMap[existingFileID]
			fileInfoMapLock.RUnlock()

			if existingFile != nil && existingFile.IsCompleted {
				response := map[string]interface{}{
					"success":       true,
					"message":       "文件已存在，秒传成功",
					"instantUpload": true,
					"fileInfo":      existingFile,
				}
				jsonResponse(w, response)
				return
			}
		}

		// 设置文件信息
		fileInfo.CreatedAt = time.Now()
		fileInfo.IsCompleted = false
		fileInfo.ChunkCount = int((fileInfo.FileSize + chunkSize - 1) / chunkSize)

		// 保存文件信息
		fileInfoMapLock.Lock()
		fileInfoMap[fileInfo.FileID] = &fileInfo
		fileInfoMapLock.Unlock()

		// 创建文件的临时目录
		chunkDir := filepath.Join(tempDir, fileInfo.FileID)
		if err := os.MkdirAll(chunkDir, 0755); err != nil {
			http3Error(w, "创建文件临时目录失败", 500)
			return
		}

		response := map[string]interface{}{
			"success":       true,
			"message":       "初始化上传成功",
			"instantUpload": false,
			"fileInfo":      fileInfo,
		}
		jsonResponse(w, response)
	})

	// 获取已上传的分片信息
	mux.HandleFunc("/api/getUploadedChunks", func(w http.ResponseWriter, r *http.Request) {
		fileID := r.URL.Query().Get("fileId")
		if fileID == "" {
			http3Error(w, "缺少文件ID", 400)
			return
		}

		fileInfoMapLock.RLock()
		fileInfo, exists := fileInfoMap[fileID]
		fileInfoMapLock.RUnlock()

		if !exists {
			http3Error(w, "文件信息不存在", 404)
			return
		}

		// 获取已上传的分片列表
		chunkDir := filepath.Join(tempDir, fileID)
		uploadedChunks := []int{}

		// 检查目录是否存在
		if _, err := os.Stat(chunkDir); !os.IsNotExist(err) {
			files, err := os.ReadDir(chunkDir)
			if err != nil {
				http3Error(w, "读取分片目录失败", 500)
				return
			}

			for _, file := range files {
				if !file.IsDir() {
					chunkNum, err := strconv.Atoi(file.Name())
					if err == nil {
						uploadedChunks = append(uploadedChunks, chunkNum)
					}
				}
			}
		}

		response := map[string]interface{}{
			"success":        true,
			"fileInfo":       fileInfo,
			"uploadedChunks": uploadedChunks,
		}
		jsonResponse(w, response)
	})

	// 上传分片处理
	mux.HandleFunc("/api/uploadChunk", func(w http.ResponseWriter, r *http.Request) {
		fileID := r.URL.Query().Get("fileId")
		chunkNumStr := r.URL.Query().Get("chunkNum")

		if fileID == "" || chunkNumStr == "" {
			http3Error(w, "缺少必要参数", 400)
			return
		}

		chunkNum, err := strconv.Atoi(chunkNumStr)
		if err != nil {
			http3Error(w, "无效的分片序号", 400)
			return
		}

		fileInfoMapLock.RLock()
		_, exists := fileInfoMap[fileID]
		fileInfoMapLock.RUnlock()

		if !exists {
			http3Error(w, "文件信息不存在", 404)
			return
		}

		// 保存分片文件
		chunkPath := filepath.Join(tempDir, fileID, chunkNumStr)
		out, err := os.Create(chunkPath)
		if err != nil {
			http3Error(w, "创建分片文件失败", 500)
			return
		}
		defer out.Close()

		// 使用QUIC流读取数据
		_, err = io.Copy(out, r.Body)
		if err != nil {
			http3Error(w, "保存分片数据失败", 500)
			return
		}

		response := map[string]interface{}{
			"success":  true,
			"message":  fmt.Sprintf("分片 %d 上传成功", chunkNum),
			"fileId":   fileID,
			"chunkNum": chunkNum,
		}
		jsonResponse(w, response)
	})

	// 完成上传，合并分片
	mux.HandleFunc("/api/completeUpload", func(w http.ResponseWriter, r *http.Request) {
		var completeReq struct {
			FileID string `json:"fileId"`
		}

		if err := json.NewDecoder(r.Body).Decode(&completeReq); err != nil {
			http3Error(w, "无效的请求数据", 400)
			return
		}

		fileID := completeReq.FileID
		fileInfoMapLock.RLock()
		fileInfo, exists := fileInfoMap[fileID]
		fileInfoMapLock.RUnlock()

		if !exists {
			http3Error(w, "文件信息不存在", 404)
			return
		}

		// 使用工作协程池处理文件合并和校验
		err := pool.Submit(func() {
			mergeChunksAndVerify(fileInfo)
		})

		if err != nil {
			http3Error(w, "提交合并任务失败", 500)
			return
		}

		response := map[string]interface{}{
			"success": true,
			"message": "文件合并任务已提交",
			"fileId":  fileID,
		}
		jsonResponse(w, response)
	})

	// 文件状态查询
	mux.HandleFunc("/api/fileStatus", func(w http.ResponseWriter, r *http.Request) {
		fileID := r.URL.Query().Get("fileId")
		if fileID == "" {
			http3Error(w, "缺少文件ID", 400)
			return
		}

		fileInfoMapLock.RLock()
		fileInfo, exists := fileInfoMap[fileID]
		fileInfoMapLock.RUnlock()

		if !exists {
			http3Error(w, "文件信息不存在", 404)
			return
		}

		response := map[string]interface{}{
			"success":  true,
			"fileInfo": fileInfo,
		}
		jsonResponse(w, response)
	})

	return mux
}

// 合并分片并验证文件完整性
func mergeChunksAndVerify(fileInfo *FileInfo) {
	log.Printf("开始合并文件: %s (%s)", fileInfo.FileName, fileInfo.FileID)

	// 创建目标文件
	destPath := filepath.Join(uploadDir, fileInfo.FileName)
	destFile, err := os.Create(destPath)
	if err != nil {
		log.Printf("创建目标文件失败: %v", err)
		return
	}
	defer destFile.Close()

	// 创建MD5哈希计算器
	hash := md5.New()

	// 合并所有分片
	chunkDir := filepath.Join(tempDir, fileInfo.FileID)
	var totalWritten int64

	for i := 0; i < fileInfo.ChunkCount; i++ {
		chunkPath := filepath.Join(chunkDir, strconv.Itoa(i))

		// 检查分片是否存在
		if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
			log.Printf("分片文件不存在: %s", chunkPath)
			return
		}

		// 打开分片文件
		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			log.Printf("打开分片文件失败: %v", err)
			return
		}

		// 写入目标文件并计算哈希
		written, err := io.Copy(io.MultiWriter(destFile, hash), chunkFile)
		chunkFile.Close()

		if err != nil {
			log.Printf("合并分片失败: %v", err)
			return
		}

		totalWritten += written
	}

	// 验证文件大小
	if totalWritten != fileInfo.FileSize {
		log.Printf("文件大小不匹配, 预期: %d, 实际: %d", fileInfo.FileSize, totalWritten)
		// 删除不完整的文件
		os.Remove(destPath)
		return
	}

	// 验证文件哈希
	calculatedHash := hex.EncodeToString(hash.Sum(nil))
	if calculatedHash != fileInfo.FileHash {
		log.Printf("文件哈希不匹配, 预期: %s, 计算得: %s", fileInfo.FileHash, calculatedHash)
		// 删除不完整的文件
		os.Remove(destPath)
		return
	}

	// 更新文件状态
	fileInfoMapLock.Lock()
	fileInfo.IsCompleted = true
	fileInfoMapLock.Unlock()

	// 更新哈希映射表用于秒传
	fileHashLock.Lock()
	fileHashMap[fileInfo.FileHash] = fileInfo.FileID
	fileHashLock.Unlock()

	log.Printf("文件合并完成并验证通过: %s", fileInfo.FileName)

	// 清理临时分片文件
	go func() {
		err := os.RemoveAll(chunkDir)
		if err != nil {
			log.Printf("清理临时分片失败: %v", err)
		}
	}()
}

// 发送JSON响应
func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("编码JSON响应失败: %v", err)
	}
}

// 发送HTTP错误响应
func http3Error(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	resp := map[string]interface{}{
		"success": false,
		"message": message,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("编码错误响应失败: %v", err)
	}
}
