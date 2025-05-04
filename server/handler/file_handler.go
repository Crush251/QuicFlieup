package handler

import (
	"QuicFlieup/server/model"
	"QuicFlieup/server/service"
	"QuicFlieup/server/utils"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

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
	FileName   string `json:"fileName"`
	FileSize   int64  `json:"fileSize"`
	FileHash   string `json:"fileHash"`
	ChunkCount int    `json:"chunkCount"`
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

	// 解析请求体
	var req FileUploadInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "无效的请求数据",
		})
		return
	}

	ctx := r.Context()

	// 检查是否可以秒传（通过Redis中的文件哈希）
	fileID, err := h.redisService.GetFileIDByHash(ctx, req.FileHash)
	if err == nil && fileID != "" {
		// 从数据库中获取文件信息
		file, err := model.GetFileByID(h.dbService.DB, fileID)
		if err == nil && file != nil && file.IsCompleted {
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
				utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"message": "创建文件记录失败: " + err.Error(),
				})
				return
			}

			// 添加到用户的文件列表
			h.redisService.AddFileToUser(ctx, userID, newFileID)

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
		}
	}

	// 生成文件ID
	newFileID := uuid.New().String()

	// 创建文件信息
	fileInfo := &model.FileInfo{
		FileID:      newFileID,
		UserID:      userID,
		FileName:    req.FileName,
		FileSize:    req.FileSize,
		FileHash:    req.FileHash,
		ChunkCount:  req.ChunkCount,
		IsCompleted: false,
	}

	// 保存到数据库
	if err := model.CreateFile(h.dbService.DB, fileInfo); err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "创建文件记录失败: " + err.Error(),
		})
		return
	}

	// 保存文件信息到Redis
	if err := h.redisService.SaveFileInfo(ctx, newFileID, fileInfo); err != nil {
		// 仅记录错误，不中断流程
		fmt.Printf("保存文件信息到Redis失败: %v\n", err)
	}

	// 添加到用户的文件列表
	h.redisService.AddFileToUser(ctx, userID, newFileID)

	// 创建文件的临时目录
	chunkDir := filepath.Join(h.tempDir, newFileID)
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "创建临时目录失败: " + err.Error(),
		})
		return
	}

	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success":       true,
		"message":       "初始化上传成功",
		"instantUpload": false,
		"fileInfo": map[string]interface{}{
			"fileId":      newFileID,
			"fileName":    req.FileName,
			"fileSize":    req.FileSize,
			"fileHash":    req.FileHash,
			"chunkCount":  req.ChunkCount,
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
	_, ok := r.Context().Value("userID").(int)
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

	ctx := r.Context()

	// 检查分片是否已上传
	isUploaded, err := h.redisService.IsChunkUploaded(ctx, fileID, chunkNum)
	if err == nil && isUploaded {
		utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
			"success":  true,
			"message":  "分片已存在",
			"chunkNum": chunkNum,
		})
		return
	}

	// 获取上传的文件
	file, _, err := r.FormFile("chunk")
	if err != nil {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "获取上传文件失败: " + err.Error(),
		})
		return
	}
	defer file.Close()

	// 创建分片文件
	chunkDir := filepath.Join(h.tempDir, fileID)
	chunkPath := filepath.Join(chunkDir, strconv.Itoa(chunkNum))

	// 确保目录存在
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "创建目录失败: " + err.Error(),
		})
		return
	}

	// 创建目标文件
	out, err := os.Create(chunkPath)
	if err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "创建文件失败: " + err.Error(),
		})
		return
	}
	defer out.Close()

	// 写入文件
	_, err = io.Copy(out, file)
	if err != nil {
		os.Remove(chunkPath) // 删除不完整的文件
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "写入文件失败: " + err.Error(),
		})
		return
	}

	// 确保写入磁盘
	err = out.Sync()
	if err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "同步文件失败: " + err.Error(),
		})
		return
	}

	// 更新Redis中的分片记录
	if err := h.redisService.AddUploadedChunk(ctx, fileID, chunkNum); err != nil {
		// 仅记录错误，不中断流程
		fmt.Printf("更新Redis分片记录失败: %v\n", err)
	}

	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success":  true,
		"message":  "分片上传成功",
		"chunkNum": chunkNum,
	})
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

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "无效的请求数据",
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

	// 获取文件信息
	fileInfo, err := model.GetFileByID(h.dbService.DB, req.FileID)
	if err != nil || fileInfo == nil {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "文件信息不存在",
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

	ctx := r.Context()

	// 验证所有分片是否都已上传
	chunks, err := h.redisService.GetUploadedChunks(ctx, req.FileID)
	if err != nil || len(chunks) != fileInfo.ChunkCount {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("分片不完整, 已上传 %d/%d", len(chunks), fileInfo.ChunkCount),
		})
		return
	}

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
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "提交合并任务失败: " + err.Error(),
		})
		return
	}

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
