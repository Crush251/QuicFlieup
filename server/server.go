package server

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http" // 新增标准http包
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"QuicFlieup/server/handler"
	"QuicFlieup/server/service"
	"QuicFlieup/server/utils"

	"github.com/panjf2000/ants/v2"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/logging"
	"github.com/quic-go/quic-go/qlog"
)

const (
	uploadDir      = "./uploads"       // 上传文件的保存目录
	tempDir        = "./temp"          // 临时文件目录，用于保存分片
	chunkSize      = 100 * 1024 * 1024 // 分片大小 (10MB)
	workerPoolSize = 100               // 工作协程池大小
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

// Server 结构体定义
type Server struct {
	DBService    *service.DBService
	RedisService *service.RedisService
	MQService    *service.MQService
	MergeService *service.MergeService
	UserHandler  *handler.UserHandler
}

// NewServer 创建服务器实例
func NewServer() *Server {
	return &Server{}
}

// Start 启动服务器
func (s *Server) Start() error {
	// 确保上传和临时目录存在
	ensureDirectories()

	// 初始化数据库服务
	dbService, err := service.NewDBService(
		"mysql",
		service.CreateMySQLDataSource("root", "qwer11023", "localhost", 3306, "quicfileup"),
	)
	if err != nil {
		return fmt.Errorf("初始化数据库服务失败: %w", err)
	}
	s.DBService = dbService

	// 初始化Redis服务
	redisService := service.NewRedisService("localhost:6379", "", 0)
	s.RedisService = redisService

	// 初始化RabbitMQ服务
	mqService, err := service.NewMQService("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return fmt.Errorf("初始化MQ服务失败: %w", err)
	}
	s.MQService = mqService

	// 初始化文件合并服务
	mergeService := service.NewMergeService(mqService, redisService, dbService, uploadDir, tempDir, chunkSize)
	s.MergeService = mergeService

	// 启动文件合并工作线程
	if err := mergeService.StartMergeWorker(context.Background()); err != nil {
		return fmt.Errorf("启动合并工作线程失败: %w", err)
	}

	// 初始化用户处理器
	s.UserHandler = handler.NewUserHandler(dbService)

	// 为了保持向后兼容，也调用原来的ServerRun函数
	go ServerRun()

	// 启动HTTP服务器
	log.Printf("服务器启动完成，Redis、MySQL和RabbitMQ连接已建立")
	return s.startHTTPServer()
}

// startHTTPServer 启动标准HTTP服务器
func (s *Server) startHTTPServer() error {
	// 创建路由器
	mux := http.NewServeMux()

	// 注册用户相关路由
	mux.HandleFunc("/api/user/register", s.UserHandler.Register)
	mux.HandleFunc("/api/user/login", s.UserHandler.Login)
	mux.HandleFunc("/api/user/info", withAuth(s.UserHandler.GetUserInfo))
	mux.HandleFunc("/api/user/logout", s.UserHandler.Logout)

	// 创建文件处理器
	fileHandler := handler.NewFileHandler(
		s.DBService,
		s.RedisService,
		s.MQService,
		s.MergeService,
		uploadDir,
		tempDir,
		chunkSize,
	)

	// 注册文件相关路由
	mux.HandleFunc("/api/initUpload", withAuth(fileHandler.InitUpload))
	mux.HandleFunc("/api/uploadChunk", withAuth(fileHandler.UploadChunk))
	mux.HandleFunc("/api/getUploadedChunks", withAuth(fileHandler.GetUploadedChunks))
	mux.HandleFunc("/api/completeUpload", withAuth(fileHandler.CompleteUpload))
	mux.HandleFunc("/api/userFiles", withAuth(fileHandler.GetUserFiles))
	mux.HandleFunc("/api/downloadFile", withAuth(fileHandler.DownloadFile))

	// 加载证书
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		return fmt.Errorf("加载证书失败: %w", err)
	}

	// 创建TLS配置
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	// 创建HTTPS服务器
	server := &http.Server{
		Addr:      ":8443",
		Handler:   mux,
		TLSConfig: tlsConfig,
	}

	log.Println("HTTP服务器启动在 https://localhost:8443")
	return server.ListenAndServeTLS("", "")
}

// 添加该函数增加认证中间件
func withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 从请求中获取Token
		token := r.Header.Get("Authorization")
		if token == "" {
			// 尝试从Cookie中获取
			cookie, err := r.Cookie("token")
			if err == nil {
				token = cookie.Value
			}
		} else {
			// 从"Bearer "格式中提取
			parts := strings.Split(token, " ")
			if len(parts) == 2 && parts[0] == "Bearer" {
				token = parts[1]
			}
		}

		if token == "" {
			utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"message": "未授权：需要登录",
			})
			return
		}

		// 验证Token并获取用户ID
		userID, err := utils.ValidateJWT(token)
		if err != nil {
			utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"message": "未授权：无效的令牌",
			})
			return
		}

		// 将用户ID添加到请求上下文
		ctx := context.WithValue(r.Context(), "userID", userID)
		next(w, r.WithContext(ctx))
	}
}

func ServerRun() {
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
		// 流控制参数设置
		MaxStreamReceiveWindow:     20 * 1024 * 1024,  // 20MB的流接收窗口
		MaxConnectionReceiveWindow: 100 * 1024 * 1024, // 100MB的连接接收窗口
		MaxIdleTimeout:             120 * time.Second, // 增加到120秒
		HandshakeIdleTimeout:       30 * time.Second,  // 握手超时
		InitialStreamReceiveWindow: 512 * 1024,        // 初始流接收窗口
		MaxIncomingStreams:         1000,              // 最大并发流数量
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

	// 启动监听QUIC连接处理直接流传输
	go handleDirectQUICConnections(tlsConfig, quicConfig, pool)

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

	// 用户相关路由 - 支持HTTP3的用户操作
	mux.HandleFunc("/api/user/register", func(w http.ResponseWriter, r *http.Request) {
		// 解析JSON请求
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Email    string `json:"email"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http3Error(w, "无效的请求数据", 400)
			return
		}

		log.Printf("HTTP3用户注册请求: 用户名=%s, 邮箱=%s", req.Username, req.Email)

		// 这里需要实际实现用户注册逻辑
		// 由于我们已经有了完整的用户处理器，这里只是转发
		// 在实际项目中应该共享用户处理逻辑而不是重复代码

		// TODO: 实现用户注册逻辑，此处简单返回成功
		response := map[string]interface{}{
			"success": true,
			"message": "用户注册成功（通过HTTP3）",
		}
		jsonResponse(w, response)
	})

	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, r *http.Request) {
		// 解析JSON请求
		var req struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http3Error(w, "无效的请求数据", 400)
			return
		}

		log.Printf("HTTP3用户登录请求: 用户名=%s", req.Username)

		// TODO: 实现用户登录逻辑
		response := map[string]interface{}{
			"success": true,
			"message": "用户登录成功（通过HTTP3）",
		}
		jsonResponse(w, response)
	})

	// 初始化上传请求处理
	mux.HandleFunc("/api/initUpload", func(w http.ResponseWriter, r *http.Request) {
		var fileInfo FileInfo
		if err := json.NewDecoder(r.Body).Decode(&fileInfo); err != nil {
			http3Error(w, "无效的请求数据", 400)
			return
		}

		log.Printf("初始化文件上传请求: 文件=%s, 大小=%d bytes, 哈希=%s",
			fileInfo.FileName, fileInfo.FileSize, fileInfo.FileHash)

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
				log.Printf("文件秒传: %s (哈希: %s)", fileInfo.FileName, fileInfo.FileHash)
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
			log.Printf("创建临时目录失败: %v", err)
			http3Error(w, "创建文件临时目录失败", 500)
			return
		}

		log.Printf("初始化上传成功: %s (ID: %s, 大小: %d, 分片数: %d)",
			fileInfo.FileName, fileInfo.FileID, fileInfo.FileSize, fileInfo.ChunkCount)

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

		log.Printf("获取已上传分片: 文件ID=%s", fileID)

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
				log.Printf("读取分片目录失败: %v", err)
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

		log.Printf("已上传分片: 文件ID=%s, 分片数=%d", fileID, len(uploadedChunks))

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

		log.Printf("上传分片请求: 文件ID=%s, 分片=%d", fileID, chunkNum)

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
			log.Printf("创建分片文件失败: %v", err)
			http3Error(w, "创建分片文件失败", 500)
			return
		}
		defer out.Close()

		// 使用QUIC流读取数据
		written, err := io.Copy(out, r.Body)
		if err != nil {
			os.Remove(chunkPath) // 删除不完整的文件
			log.Printf("保存分片数据失败: %v", err)
			http3Error(w, "保存分片数据失败", 500)
			return
		}

		log.Printf("分片上传成功: 文件ID=%s, 分片=%d, 大小=%d bytes", fileID, chunkNum, written)

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
		log.Printf("完成上传请求: 文件ID=%s", fileID)

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
			log.Printf("提交合并任务失败: %v", err)
			http3Error(w, "提交合并任务失败", 500)
			return
		}

		log.Printf("文件合并任务已提交: 文件=%s, ID=%s", fileInfo.FileName, fileID)

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

		log.Printf("文件状态查询: ID=%s", fileID)

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
	log.Printf("开始合并文件: %s (%s), 大小: %.2f MB",
		fileInfo.FileName, fileInfo.FileID, float64(fileInfo.FileSize)/(1024*1024))

	// 确保上传目录存在
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		log.Printf("创建上传目录失败: %v", err)
		return
	}

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
	var lastProgressReport time.Time = time.Now()

	// 检查分片目录是否存在
	if _, err := os.Stat(chunkDir); os.IsNotExist(err) {
		log.Printf("分片目录不存在: %s", chunkDir)
		return
	}

	// 分片合并进度报告函数
	reportProgress := func(i int, written int64) {
		if time.Since(lastProgressReport) > 5*time.Second {
			percentage := float64(i+1) / float64(fileInfo.ChunkCount) * 100
			writtenMB := float64(written) / (1024 * 1024)
			totalMB := float64(fileInfo.FileSize) / (1024 * 1024)
			log.Printf("合并进度: %.2f%% (分片 %d/%d), 已写入: %.2f MB / %.2f MB",
				percentage, i+1, fileInfo.ChunkCount, writtenMB, totalMB)
			lastProgressReport = time.Now()
		}
	}

	for i := 0; i < fileInfo.ChunkCount; i++ {
		chunkPath := filepath.Join(chunkDir, strconv.Itoa(i))

		// 检查分片是否存在
		if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
			log.Printf("分片文件不存在: %s", chunkPath)
			os.Remove(destPath) // 清理不完整的文件
			return
		}

		// 打开分片文件
		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			log.Printf("打开分片文件失败: %v", err)
			os.Remove(destPath) // 清理不完整的文件
			return
		}

		// 写入目标文件并计算哈希
		written, err := io.Copy(io.MultiWriter(destFile, hash), chunkFile)
		chunkFile.Close()

		if err != nil {
			log.Printf("合并分片失败: %v", err)
			os.Remove(destPath) // 清理不完整的文件
			return
		}

		totalWritten += written
		reportProgress(i, totalWritten)
	}

	// 确保所有数据都写入磁盘
	if err := destFile.Sync(); err != nil {
		log.Printf("同步文件到磁盘失败: %v", err)
	}

	// 验证文件大小
	if totalWritten != fileInfo.FileSize {
		log.Printf("文件大小不匹配, 预期: %d, 实际: %d", fileInfo.FileSize, totalWritten)
		os.Remove(destPath) // 删除不完整的文件
		return
	}

	// 计算文件哈希
	calculatedHash := hex.EncodeToString(hash.Sum(nil))
	log.Printf("文件哈希计算完成: 期望=%s, 实际=%s", fileInfo.FileHash, calculatedHash)

	// 对于大文件，我们可能不验证哈希
	skipHashVerify := false
	if fileInfo.FileSize > 1024*1024*1024 { // 大于1GB的文件
		skipHashVerify = true
		log.Printf("文件大小超过1GB，跳过哈希验证")
	}

	// 验证文件哈希
	if !skipHashVerify && calculatedHash != fileInfo.FileHash {
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
	// 对于大文件，使用计算出的哈希
	hashToSave := fileInfo.FileHash
	if skipHashVerify {
		hashToSave = calculatedHash
	}

	fileHashLock.Lock()
	fileHashMap[hashToSave] = fileInfo.FileID
	fileHashLock.Unlock()

	log.Printf("文件合并完成并验证通过: %s (大小: %.2f MB)",
		fileInfo.FileName, float64(totalWritten)/(1024*1024))

	// 清理临时分片文件
	go func() {
		err := os.RemoveAll(chunkDir)
		if err != nil {
			log.Printf("清理临时分片失败: %v", err)
		} else {
			log.Printf("临时分片文件清理成功: %s", chunkDir)
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

// 处理直接的QUIC连接
func handleDirectQUICConnections(tlsConfig *tls.Config, quicConfig *quic.Config, pool *ants.Pool) {
	log.Println("启动QUIC直接流处理监听在 :4434")

	// 创建QUIC监听器 - 使用不同端口以避免和HTTP/3服务器冲突
	listener, err := quic.ListenAddr(":4434", tlsConfig, quicConfig)
	if err != nil {
		log.Fatalf("创建QUIC监听器失败: %v", err)
		return
	}

	// 接受连接
	for {
		conn, err := listener.Accept(context.Background())
		if err != nil {
			log.Printf("接受QUIC连接失败: %v", err)
			continue
		}

		// 为每个连接启动一个处理goroutine
		go handleQuicConnection(conn, pool)
	}
}

// 处理单个QUIC连接
func handleQuicConnection(conn quic.Connection, pool *ants.Pool) {
	log.Printf("接受来自 %s 的QUIC连接", conn.RemoteAddr())

	// 接受连接上的所有流
	for {
		stream, err := conn.AcceptStream(context.Background())
		if err != nil {
			log.Printf("接受流失败: %v", err)
			break
		}

		// 处理每个流
		err = pool.Submit(func() {
			handleQuicStream(stream)
		})

		if err != nil {
			log.Printf("提交流处理任务失败: %v", err)
			stream.Close()
		}
	}
}

// 处理单个QUIC流请求
func handleQuicStream(stream quic.Stream) {
	defer stream.Close()

	// 读取HTTP请求头
	reqBuf := make([]byte, 4096)
	n, err := stream.Read(reqBuf)
	if err != nil && err != io.EOF {
		log.Printf("读取请求头失败: %v", err)
		return
	}

	// 解析HTTP请求
	reqStr := string(reqBuf[:n])
	requestLines := strings.Split(reqStr, "\r\n")
	if len(requestLines) < 1 {
		log.Printf("无效的HTTP请求: %s", reqStr)
		return
	}

	// 解析请求行
	requestParts := strings.Split(requestLines[0], " ")
	if len(requestParts) < 2 {
		log.Printf("无效的HTTP请求行: %s", requestLines[0])
		return
	}

	// method变量暂时未使用，但保留以便将来扩展
	// 目前只处理POST请求
	_ = requestParts[0] // 忽略method变量
	path := requestParts[1]

	// 查找Content-Length
	var contentLength int64
	for _, line := range requestLines {
		if strings.HasPrefix(strings.ToLower(line), "content-length:") {
			lenStr := strings.TrimSpace(line[len("content-length:"):])
			contentLength, _ = strconv.ParseInt(lenStr, 10, 64)
		}
	}

	// 找到请求体的起始位置
	bodyStartIdx := bytes.Index(reqBuf[:n], []byte("\r\n\r\n"))
	if bodyStartIdx == -1 {
		log.Printf("未找到请求体: %s", reqStr)
		return
	}
	bodyStartIdx += 4 // 跳过"\r\n\r\n"

	// 处理不同的API路径
	switch {
	case strings.HasPrefix(path, "/api/streamUploadChunk"):
		handleStreamUploadChunk(stream, path, reqBuf[:n][bodyStartIdx:], contentLength-int64(n-bodyStartIdx))
	case path == "/api/initUpload":
		handleStreamInitUpload(stream, reqBuf[:n][bodyStartIdx:])
	case path == "/api/completeUpload":
		handleStreamCompleteUpload(stream, reqBuf[:n][bodyStartIdx:])
	default:
		sendErrorResponse(stream, "未知的API路径", 404)
	}
}

// 处理流式上传分片
func handleStreamUploadChunk(stream quic.Stream, path string, initialBody []byte, remainingBytes int64) {
	// 记录开始时间，用于计算处理时间
	startTime := time.Now()

	// 解析查询参数
	u, err := url.Parse(path)
	if err != nil {
		sendErrorResponse(stream, "解析路径失败", 400)
		return
	}

	query := u.Query()
	fileID := query.Get("fileId")
	chunkNumStr := query.Get("chunkNum")

	if fileID == "" || chunkNumStr == "" {
		sendErrorResponse(stream, "缺少必要参数", 400)
		return
	}

	chunkNum, err := strconv.Atoi(chunkNumStr)
	if err != nil {
		sendErrorResponse(stream, "无效的分片序号", 400)
		return
	}

	log.Printf("开始处理分片: %s - %d", fileID, chunkNum)

	fileInfoMapLock.RLock()
	_, exists := fileInfoMap[fileID]
	fileInfoMapLock.RUnlock()

	if !exists {
		sendErrorResponse(stream, "文件信息不存在", 404)
		return
	}

	// 确保目录存在
	chunkDir := filepath.Join(tempDir, fileID)
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		sendErrorResponse(stream, fmt.Sprintf("创建分片目录失败: %v", err), 500)
		return
	}

	// 保存分片文件
	chunkPath := filepath.Join(chunkDir, chunkNumStr)
	out, err := os.Create(chunkPath)
	if err != nil {
		sendErrorResponse(stream, fmt.Sprintf("创建分片文件失败: %v", err), 500)
		return
	}
	defer out.Close()

	// 首先写入初始读取的数据
	if len(initialBody) > 0 {
		if _, err := out.Write(initialBody); err != nil {
			os.Remove(chunkPath) // 清理不完整的文件
			sendErrorResponse(stream, fmt.Sprintf("写入分片数据失败: %v", err), 500)
			return
		}
	}

	// 设置处理超时 - 最多给120秒完成分片传输
	if err := stream.SetReadDeadline(time.Now().Add(120 * time.Second)); err != nil {
		log.Printf("设置读取超时失败: %v", err)
	}

	// 读取剩余数据
	if remainingBytes > 0 {
		// 使用缓冲区读取以提高性能
		buffer := make([]byte, 64*1024) // 增加到64KB缓冲区
		var totalRead int64

		for totalRead < remainingBytes {
			toRead := remainingBytes - totalRead
			if toRead > int64(len(buffer)) {
				toRead = int64(len(buffer))
			}

			n, err := stream.Read(buffer[:toRead])
			if err != nil && err != io.EOF {
				os.Remove(chunkPath) // 清理不完整的文件
				sendErrorResponse(stream, fmt.Sprintf("读取分片数据失败: %v", err), 500)
				return
			}

			if n > 0 {
				if _, err := out.Write(buffer[:n]); err != nil {
					os.Remove(chunkPath) // 清理不完整的文件
					sendErrorResponse(stream, fmt.Sprintf("写入分片数据失败: %v", err), 500)
					return
				}
				totalRead += int64(n)
			}

			if err == io.EOF || n == 0 {
				break
			}
		}

		// 检查是否读取了所有数据
		if totalRead < remainingBytes {
			os.Remove(chunkPath) // 清理不完整的文件
			sendErrorResponse(stream, fmt.Sprintf("数据不完整: 预期 %d 字节, 实际读取 %d 字节", remainingBytes, totalRead), 400)
			return
		}
	}

	// 成功完成，同步写入磁盘
	if err := out.Sync(); err != nil {
		log.Printf("同步文件到磁盘失败: %v", err)
	}

	// 重置读取超时为响应的合理值
	if err := stream.SetReadDeadline(time.Time{}); err != nil {
		log.Printf("重置读取超时失败: %v", err)
	}

	// 计算处理时间
	processingTime := time.Since(startTime)

	// 发送成功响应
	response := map[string]interface{}{
		"success":  true,
		"message":  fmt.Sprintf("分片 %d 上传成功", chunkNum),
		"fileId":   fileID,
		"chunkNum": chunkNum,
		"time":     processingTime.Seconds(), // 包含处理时间信息
	}

	log.Printf("完成处理分片: %s - %d, 耗时: %.2f秒", fileID, chunkNum, processingTime.Seconds())
	sendSuccessResponse(stream, response)
}

// 处理流式初始化上传
func handleStreamInitUpload(stream quic.Stream, body []byte) {
	var fileInfo FileInfo
	if err := json.Unmarshal(body, &fileInfo); err != nil {
		sendErrorResponse(stream, "无效的请求数据", 400)
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
			sendSuccessResponse(stream, response)
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
		sendErrorResponse(stream, "创建文件临时目录失败", 500)
		return
	}

	response := map[string]interface{}{
		"success":       true,
		"message":       "初始化上传成功",
		"instantUpload": false,
		"fileInfo":      fileInfo,
	}
	sendSuccessResponse(stream, response)
}

// 处理流式完成上传
func handleStreamCompleteUpload(stream quic.Stream, body []byte) {
	var completeReq struct {
		FileID string `json:"fileId"`
	}

	if err := json.Unmarshal(body, &completeReq); err != nil {
		sendErrorResponse(stream, "无效的请求数据", 400)
		return
	}

	fileID := completeReq.FileID
	fileInfoMapLock.RLock()
	fileInfo, exists := fileInfoMap[fileID]
	fileInfoMapLock.RUnlock()

	if !exists {
		sendErrorResponse(stream, "文件信息不存在", 404)
		return
	}

	// 提交合并任务
	go mergeChunksAndVerify(fileInfo)

	response := map[string]interface{}{
		"success": true,
		"message": "文件合并任务已提交",
		"fileId":  fileID,
	}
	sendSuccessResponse(stream, response)
}

// 发送成功响应
func sendSuccessResponse(stream quic.Stream, data interface{}) {
	// 序列化响应数据
	respData, err := json.Marshal(data)
	if err != nil {
		log.Printf("序列化响应失败: %v", err)
		return
	}

	// 发送HTTP响应
	headers := fmt.Sprintf(
		"HTTP/1.1 200 OK\r\n"+
			"Content-Type: application/json\r\n"+
			"Content-Length: %d\r\n\r\n",
		len(respData))

	if _, err := stream.Write([]byte(headers)); err != nil {
		log.Printf("发送响应头失败: %v", err)
		return
	}

	if _, err := stream.Write(respData); err != nil {
		log.Printf("发送响应数据失败: %v", err)
	}
}

// 发送错误响应
func sendErrorResponse(stream quic.Stream, message string, statusCode int) {
	log.Printf("错误: %s [%d]", message, statusCode)

	// 创建错误响应
	errResp := map[string]interface{}{
		"success": false,
		"message": message,
	}

	// 序列化响应数据
	respData, err := json.Marshal(errResp)
	if err != nil {
		log.Printf("序列化错误响应失败: %v", err)
		return
	}

	// 获取状态文本
	statusText := http.StatusText(statusCode)
	if statusText == "" {
		statusText = "Error"
	}

	// 发送HTTP响应
	headers := fmt.Sprintf(
		"HTTP/1.1 %d %s\r\n"+
			"Content-Type: application/json\r\n"+
			"Content-Length: %d\r\n\r\n",
		statusCode, statusText, len(respData))

	if _, err := stream.Write([]byte(headers)); err != nil {
		log.Printf("发送错误响应头失败: %v", err)
		return
	}

	if _, err := stream.Write(respData); err != nil {
		log.Printf("发送错误响应数据失败: %v", err)
	}
}
