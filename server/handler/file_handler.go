package handler

import (
	"QuicFlieup/server/model"
	"QuicFlieup/server/service"
	"QuicFlieup/server/utils"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"bytes"

	"github.com/google/uuid"
)

// FileHandler 文件处理器
type FileHandler struct {
	dbService    *service.DBService
	redisService *service.RedisService
	mqService    *service.MQService
	mergeService *service.MergeService
	uploadDir    string
	tempDir      string
	chunkSize    int64
}

// FileUploadInitRequest 文件上传初始化请求
type FileUploadInitRequest struct {
	FileID     string `json:"fileId"`     // 客户端指定的文件ID，可选
	FileName   string `json:"fileName"`   // 文件名
	FileSize   int64  `json:"fileSize"`   // 文件大小
	FileHash   string `json:"fileHash"`   // 文件MD5哈希值
	ChunkCount int    `json:"chunkCount"` // 分片总数
}

// ChunkUploadRequest 分片上传请求
type ChunkUploadRequest struct {
	FileID    string `json:"fileId"`
	ChunkNum  int    `json:"chunkNum"`
	TotalSize int64  `json:"totalSize"`
}

// NewFileHandler 创建新的文件处理器
func NewFileHandler(
	dbService *service.DBService,
	redisService *service.RedisService,
	mqService *service.MQService,
	mergeService *service.MergeService,
	uploadDir, tempDir string,
	chunkSize int64,
) *FileHandler {
	return &FileHandler{
		dbService:    dbService,
		redisService: redisService,
		mqService:    mqService,
		mergeService: mergeService,
		uploadDir:    uploadDir,
		tempDir:      tempDir,
		chunkSize:    chunkSize,
	}
}

// InitUpload 初始化文件上传
func (h *FileHandler) InitUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		utils.JSONResponse(w, http.StatusMethodNotAllowed, map[string]interface{}{
			"success": false,
			"message": "只支持POST请求",
		})
		return
	}

	// 从上下文中获取用户ID
	userID, ok := r.Context().Value("userID").(int)
	if !ok {
		utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"message": "未授权",
		})
		return
	}

	ctx := r.Context()

	// 记录原始请求内容
	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("读取请求体失败: %v", err)
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "读取请求体失败",
		})
		return
	}
	r.Body = io.NopCloser(bytes.NewBuffer(requestBody))

	log.Printf("收到初始化上传请求: 用户ID=%d, 请求体=%s", userID, string(requestBody))

	// 解析请求体
	var req FileUploadInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("解析请求失败: %v, 原始内容: %s", err, string(requestBody))
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "无效的请求数据: " + err.Error(),
		})
		return
	}

	log.Printf("文件上传初始化: 用户ID=%d, 文件名=%s, 大小=%.2fMB, 哈希=%s",
		userID, req.FileName, float64(req.FileSize)/(1024*1024), req.FileHash)

	// 验证请求参数
	if req.FileName == "" || req.FileSize <= 0 || req.FileHash == "" {
		log.Printf("请求参数无效: 文件名=%s, 大小=%d, 哈希=%s",
			req.FileName, req.FileSize, req.FileHash)
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "请求参数无效",
		})
		return
	}

	// 检查是否可以秒传（通过Redis中的文件哈希）
	fileID, err := h.redisService.GetFileIDByHash(ctx, req.FileHash)
	if err == nil && fileID != "" {
		log.Printf("发现相同哈希文件: 哈希=%s, 文件ID=%s", req.FileHash, fileID)

		// 从数据库中获取文件信息
		file, err := model.GetFileByID(h.dbService.DB, fileID)
		if err == nil && file != nil && file.IsCompleted {
			log.Printf("秒传条件满足: 文件=%s, 大小=%.2fMB",
				file.FileName, float64(file.FileSize)/(1024*1024))

			// 创建一个新的文件记录，指向现有的文件
			newFileID := uuid.New().String()
			newFile := &model.FileInfo{
				FileID:      newFileID,
				UserID:      userID,
				FileName:    req.FileName, // 使用新的文件名
				FileSize:    file.FileSize,
				FileHash:    req.FileHash,
				ChunkCount:  file.ChunkCount,
				IsCompleted: true,
			}

			if err := model.CreateFile(h.dbService.DB, newFile); err != nil {
				log.Printf("创建文件记录失败: %v", err)
				utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"message": "创建文件记录失败: " + err.Error(),
				})
				return
			}

			// 保存新文件信息到Redis
			if err := h.redisService.SaveFileInfo(ctx, newFileID, newFile); err != nil {
				log.Printf("保存文件信息到Redis失败: %v", err)
				// 继续处理，不中断流程
			} else {
				log.Printf("文件信息已保存到Redis: ID=%s", newFileID)
			}

			// 添加到用户的文件列表
			if err := h.redisService.AddFileToUser(ctx, userID, newFileID); err != nil {
				log.Printf("添加到用户文件列表失败: %v", err)
				// 继续处理，不中断流程
			}

			log.Printf("秒传成功: 用户ID=%d, 文件名=%s, 新文件ID=%s",
				userID, req.FileName, newFileID)

			utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
				"success":       true,
				"message":       "文件已存在，秒传成功",
				"instantUpload": true,
				"fileInfo": map[string]interface{}{
					"fileId":      newFileID,
					"fileName":    req.FileName,
					"fileSize":    file.FileSize,
					"fileHash":    req.FileHash,
					"chunkCount":  file.ChunkCount,
					"isCompleted": true,
				},
			})
			return
		} else {
			log.Printf("文件ID存在但状态不满足秒传条件: ID=%s, 错误=%v", fileID, err)
		}
	} else {
		if err != nil {
			log.Printf("Redis中查询哈希失败: %v", err)
		} else {
			log.Printf("Redis中未找到匹配哈希: %s", req.FileHash)
		}
	}

	// 生成文件ID
	newFileID := req.FileID
	if newFileID == "" {
		newFileID = uuid.New().String()
	}

	// 计算分片数量
	chunkCount := req.ChunkCount
	if chunkCount <= 0 {
		chunkCount = int((req.FileSize + h.chunkSize - 1) / h.chunkSize)
	}

	// 创建文件信息
	fileInfo := &model.FileInfo{
		FileID:      newFileID,
		UserID:      userID,
		FileName:    req.FileName,
		FileSize:    req.FileSize,
		FileHash:    req.FileHash,
		ChunkCount:  chunkCount,
		IsCompleted: false,
	}

	log.Printf("开始新文件上传: ID=%s, 用户=%d, 文件名=%s, 分片数=%d, 哈希=%s",
		newFileID, userID, req.FileName, chunkCount, req.FileHash)

	// 保存到数据库
	if err := model.CreateFile(h.dbService.DB, fileInfo); err != nil {
		log.Printf("创建文件数据库记录失败: %v", err)
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "创建文件记录失败: " + err.Error(),
		})
		return
	}
	log.Printf("文件信息已保存到数据库: ID=%s", newFileID)

	// 保存文件信息到Redis
	if err := h.redisService.SaveFileInfo(ctx, newFileID, fileInfo); err != nil {
		// 仅记录错误，不中断流程
		log.Printf("保存文件信息到Redis失败: %v", err)
	} else {
		log.Printf("文件信息已保存到Redis: ID=%s", newFileID)
	}

	// 添加到用户的文件列表
	if err := h.redisService.AddFileToUser(ctx, userID, newFileID); err != nil {
		log.Printf("添加到用户文件列表失败: %v", err)
		// 继续处理，不中断流程
	} else {
		log.Printf("文件已添加到用户文件列表: 用户ID=%d, 文件ID=%s", userID, newFileID)
	}

	// 创建文件的临时目录
	chunkDir := filepath.Join(h.tempDir, newFileID)
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		log.Printf("创建临时目录失败: %v", err)
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "创建临时目录失败: " + err.Error(),
		})
		return
	}

	log.Printf("文件上传初始化成功: ID=%s, 用户=%d, 文件名=%s",
		newFileID, userID, req.FileName)

	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success":       true,
		"message":       "初始化上传成功",
		"instantUpload": false,
		"fileInfo": map[string]interface{}{
			"fileId":      newFileID,
			"fileName":    req.FileName,
			"fileSize":    req.FileSize,
			"fileHash":    req.FileHash,
			"chunkCount":  chunkCount,
			"isCompleted": false,
		},
	})
}

// GetUploadedChunks 获取已上传的分片
func (h *FileHandler) GetUploadedChunks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		utils.JSONResponse(w, http.StatusMethodNotAllowed, map[string]interface{}{
			"success": false,
			"message": "只支持GET请求",
		})
		return
	}

	// 获取文件ID
	fileID := r.URL.Query().Get("fileId")
	if fileID == "" {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "缺少文件ID",
		})
		return
	}

	ctx := r.Context()

	// 从Redis获取已上传的分片
	chunks, err := h.redisService.GetUploadedChunks(ctx, fileID)
	if err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "获取分片信息失败: " + err.Error(),
		})
		return
	}

	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"chunks":  chunks,
	})
}

// UploadChunk 上传文件分片
func (h *FileHandler) UploadChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		utils.JSONResponse(w, http.StatusMethodNotAllowed, map[string]interface{}{
			"success": false,
			"message": "只支持POST请求",
		})
		return
	}

	// 从上下文中获取用户ID
	userID, ok := r.Context().Value("userID").(int)
	if !ok {
		utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"message": "未授权",
		})
		return
	}

	// 限制请求体大小
	r.Body = http.MaxBytesReader(w, r.Body, h.chunkSize+1024) // 分片大小 + 额外空间用于表单数据

	// 解析多部分表单
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		log.Printf("解析表单失败: %v", err)
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "解析表单失败: " + err.Error(),
		})
		return
	}

	// 获取文件ID和分片编号
	fileID := r.FormValue("fileId")
	chunkNumStr := r.FormValue("chunkNum")

	if fileID == "" || chunkNumStr == "" {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "缺少必要参数",
		})
		return
	}

	chunkNum, err := strconv.Atoi(chunkNumStr)
	if err != nil {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "无效的分片编号",
		})
		return
	}

	log.Printf("分片上传请求: 用户ID=%d, 文件ID=%s, 分片=%d", userID, fileID, chunkNum)

	ctx := r.Context()

	// 检查分片是否已上传
	isUploaded, err := h.redisService.IsChunkUploaded(ctx, fileID, chunkNum)
	if err == nil && isUploaded {
		log.Printf("分片已存在，跳过: 文件ID=%s, 分片=%d", fileID, chunkNum)
		utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
			"success":  true,
			"message":  "分片已存在",
			"chunkNum": chunkNum,
		})
		return
	}

	// 获取上传的文件
	file, fileHeader, err := r.FormFile("chunk")
	if err != nil {
		log.Printf("获取上传文件失败: %v", err)
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "获取上传文件失败: " + err.Error(),
		})
		return
	}
	defer file.Close()

	log.Printf("接收到分片数据: 文件ID=%s, 分片=%d, 大小=%d bytes",
		fileID, chunkNum, fileHeader.Size)

	// 创建分片文件
	chunkDir := filepath.Join(h.tempDir, fileID)
	chunkPath := filepath.Join(chunkDir, strconv.Itoa(chunkNum))

	// 确保目录存在
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		log.Printf("创建目录失败: %v", err)
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "创建目录失败: " + err.Error(),
		})
		return
	}

	// 创建目标文件
	out, err := os.Create(chunkPath)
	if err != nil {
		log.Printf("创建文件失败: %v", err)
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "创建文件失败: " + err.Error(),
		})
		return
	}
	defer out.Close()

	// 写入文件
	written, err := io.Copy(out, file)
	if err != nil {
		os.Remove(chunkPath) // 删除不完整的文件
		log.Printf("写入文件失败: %v", err)
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "写入文件失败: " + err.Error(),
		})
		return
	}

	// 确保写入磁盘
	if err = out.Sync(); err != nil {
		log.Printf("同步文件失败: %v", err)
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "同步文件失败: " + err.Error(),
		})
		return
	}

	log.Printf("分片写入成功: 文件ID=%s, 分片=%d, 写入大小=%d bytes",
		fileID, chunkNum, written)

	// 更新Redis中的分片记录
	if err := h.redisService.AddUploadedChunk(ctx, fileID, chunkNum); err != nil {
		// 仅记录错误，不中断流程
		log.Printf("更新Redis分片记录失败: %v", err)
	}

	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"message":  "分片上传成功",
		"chunkNum": chunkNum,
	})
}

// 计算文件哈希值
func calculateFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// CompleteUpload 完成文件上传
func (h *FileHandler) CompleteUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		utils.JSONResponse(w, http.StatusMethodNotAllowed, map[string]interface{}{
			"success": false,
			"message": "只支持POST请求",
		})
		return
	}

	// 从上下文中获取用户ID
	userID, ok := r.Context().Value("userID").(int)
	if !ok {
		utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"message": "未授权",
		})
		return
	}

	// 解析请求体
	var req struct {
		FileID string `json:"fileId"`
	}

	// 记录原始请求内容
	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("读取请求体失败: %v", err)
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "读取请求体失败",
		})
		return
	}
	r.Body = io.NopCloser(bytes.NewBuffer(requestBody))

	log.Printf("收到完成上传请求: 用户ID=%d, 请求体=%s", userID, string(requestBody))

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("解析请求失败: %v, 原始内容: %s", err, string(requestBody))
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "无效的请求数据: " + err.Error(),
		})
		return
	}

	if req.FileID == "" {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "缺少文件ID",
		})
		return
	}

	log.Printf("文件上传完成请求: 用户ID=%d, 文件ID=%s", userID, req.FileID)

	ctx := r.Context()

	// 首先尝试从Redis获取文件信息
	var fileInfo model.FileInfo
	err = h.redisService.GetFileInfo(ctx, req.FileID, &fileInfo)
	if err != nil {
		log.Printf("从Redis获取文件信息失败, 尝试从数据库获取: %v", err)

		// 从数据库获取文件信息
		dbFileInfo, err := model.GetFileByID(h.dbService.DB, req.FileID)
		if err != nil || dbFileInfo == nil {
			log.Printf("从数据库获取文件信息失败: %v", err)
			utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"message": "文件信息不存在",
			})
			return
		}

		fileInfo = *dbFileInfo

		// 尝试将文件信息保存到Redis
		if err := h.redisService.SaveFileInfo(ctx, req.FileID, fileInfo); err != nil {
			log.Printf("将文件信息保存到Redis失败: %v", err)
			// 继续处理，不中断流程
		} else {
			log.Printf("文件信息已保存到Redis: %s", req.FileID)
		}
	} else {
		log.Printf("从Redis获取到文件信息: ID=%s, 文件名=%s", fileInfo.FileID, fileInfo.FileName)
	}

	// 检查文件是否属于当前用户
	if fileInfo.UserID != userID {
		log.Printf("文件所有权验证失败: 期望用户=%d, 实际用户=%d", userID, fileInfo.UserID)
		utils.JSONResponse(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"message": "无权访问该文件",
		})
		return
	}

	// 验证所有分片是否都已上传
	chunks, err := h.redisService.GetUploadedChunks(ctx, req.FileID)
	if err != nil {
		log.Printf("获取已上传分片失败: %v", err)

		// 检查临时目录中实际存在的分片
		chunkDir := filepath.Join(h.tempDir, req.FileID)
		if _, err := os.Stat(chunkDir); os.IsNotExist(err) {
			log.Printf("分片目录不存在: %s", chunkDir)
			utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"message": "分片目录不存在",
			})
			return
		}

		// 统计目录中的分片数量
		files, err := os.ReadDir(chunkDir)
		if err != nil {
			log.Printf("读取分片目录失败: %v", err)
			utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"message": "读取分片目录失败: " + err.Error(),
			})
			return
		}

		// 统计实际分片数量
		chunks = make([]int, 0, len(files))
		for _, file := range files {
			if !file.IsDir() {
				chunkNum, err := strconv.Atoi(file.Name())
				if err == nil {
					chunks = append(chunks, chunkNum)
				}
			}
		}

		// 将分片信息更新到Redis
		for _, chunkNum := range chunks {
			if err := h.redisService.AddUploadedChunk(ctx, req.FileID, chunkNum); err != nil {
				log.Printf("更新分片信息到Redis失败: %v", err)
				// 继续处理，不中断流程
			}
		}

		log.Printf("从文件系统统计的分片数量: %d", len(chunks))
	}

	if len(chunks) != fileInfo.ChunkCount {
		log.Printf("分片不完整: 期望=%d, 实际=%d", fileInfo.ChunkCount, len(chunks))
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("分片不完整, 已上传 %d/%d", len(chunks), fileInfo.ChunkCount),
		})
		return
	}

	log.Printf("所有分片已上传，开始合并: 文件ID=%s, 分片数=%d", req.FileID, fileInfo.ChunkCount)

	// 提交合并任务到MQ
	err = h.mergeService.PublishMergeTask(
		ctx,
		fileInfo.FileID,
		fileInfo.UserID,
		fileInfo.FileName,
		fileInfo.FileSize,
		fileInfo.FileHash,
		fileInfo.ChunkCount,
	)

	if err != nil {
		log.Printf("提交合并任务失败: %v", err)
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "提交合并任务失败: " + err.Error(),
		})
		return
	}

	log.Printf("合并任务提交成功: 文件ID=%s, 文件名=%s", fileInfo.FileID, fileInfo.FileName)

	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "文件合并任务已提交",
		"fileId":  req.FileID,
	})
}

// GetUserFiles 获取用户的文件列表
func (h *FileHandler) GetUserFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		utils.JSONResponse(w, http.StatusMethodNotAllowed, map[string]interface{}{
			"success": false,
			"message": "只支持GET请求",
		})
		return
	}

	// 从上下文中获取用户ID
	userID, ok := r.Context().Value("userID").(int)
	if !ok {
		utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"message": "未授权",
		})
		return
	}

	// 从数据库获取用户的文件列表
	files, err := model.GetUserFiles(h.dbService.DB, userID)
	if err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "获取文件列表失败: " + err.Error(),
		})
		return
	}

	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"data":    files,
	})
}

// DownloadFile 下载文件
func (h *FileHandler) DownloadFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		utils.JSONResponse(w, http.StatusMethodNotAllowed, map[string]interface{}{
			"success": false,
			"message": "只支持GET请求",
		})
		return
	}

	// 从上下文中获取用户ID
	userID, ok := r.Context().Value("userID").(int)
	if !ok {
		utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"message": "未授权",
		})
		return
	}

	// 获取文件ID
	fileID := r.URL.Query().Get("fileId")
	if fileID == "" {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "缺少文件ID",
		})
		return
	}

	// 获取文件信息
	fileInfo, err := model.GetFileByID(h.dbService.DB, fileID)
	if err != nil || fileInfo == nil {
		utils.JSONResponse(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"message": "文件不存在",
		})
		return
	}

	// 检查文件是否属于当前用户
	if fileInfo.UserID != userID {
		utils.JSONResponse(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"message": "无权访问该文件",
		})
		return
	}

	// 检查文件是否已完成合并
	if !fileInfo.IsCompleted {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "文件尚未合并完成",
		})
		return
	}

	// 文件路径
	filePath := filepath.Join(h.uploadDir, fileInfo.FileName)

	// 检查文件是否存在
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		utils.JSONResponse(w, http.StatusNotFound, map[string]interface{}{
			"success": false,
			"message": "文件不存在",
		})
		return
	}

	// 设置响应头
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, fileInfo.FileName))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(fileInfo.FileSize, 10))

	// 打开文件并发送
	file, err := os.Open(filePath)
	if err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "打开文件失败: " + err.Error(),
		})
		return
	}
	defer file.Close()

	// 将文件复制到响应
	_, err = io.Copy(w, file)
	if err != nil {
		// 无法返回错误，因为部分响应已经发送
		fmt.Printf("发送文件失败: %v\n", err)
	}
}
