package service

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"QuicFlieup/server/model"
)

// MergeService 文件合并服务
type MergeService struct {
	mqService    *MQService
	redisService *RedisService
	dbService    *DBService
	uploadDir    string // 上传文件的保存目录
	tempDir      string // 临时文件目录
	chunkSize    int64  // 分片大小
}

// NewMergeService 创建文件合并服务
func NewMergeService(mq *MQService, redis *RedisService, db *DBService, uploadDir, tempDir string, chunkSize int64) *MergeService {
	return &MergeService{
		mqService:    mq,
		redisService: redis,
		dbService:    db,
		uploadDir:    uploadDir,
		tempDir:      tempDir,
		chunkSize:    chunkSize,
	}
}

// StartMergeWorker 启动合并工作线程
func (ms *MergeService) StartMergeWorker(ctx context.Context) error {
	return ms.mqService.ConsumeFileMergeTasks(func(task FileTaskMessage) error {
		log.Printf("接收到文件合并任务: %s", task.FileID)

		// 解析任务数据
		if task.TaskType != "merge" {
			return fmt.Errorf("不支持的任务类型: %s", task.TaskType)
		}

		var mergeData MergeTaskData
		if err := json.Unmarshal(task.Data, &mergeData); err != nil {
			return err
		}

		// 执行合并
		return ms.MergeFile(ctx, task.FileID, &mergeData)
	})
}

// MergeFile 合并文件
func (ms *MergeService) MergeFile(ctx context.Context, fileID string, data *MergeTaskData) error {
	log.Printf("开始合并文件: %s (%s)", data.FileName, fileID)

	// 创建目标文件
	destPath := filepath.Join(ms.uploadDir, data.FileName)
	destFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("创建目标文件失败: %w", err)
	}
	defer destFile.Close()

	// 创建MD5哈希计算器
	hash := md5.New()

	// 合并所有分片
	chunkDir := filepath.Join(ms.tempDir, fileID)
	var totalWritten int64

	for i := 0; i < data.ChunkCount; i++ {
		chunkPath := filepath.Join(chunkDir, strconv.Itoa(i))

		// 检查分片是否存在
		if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
			return fmt.Errorf("分片文件不存在: %s", chunkPath)
		}

		// 打开分片文件
		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			return fmt.Errorf("打开分片文件失败: %w", err)
		}

		// 写入目标文件并计算哈希
		written, err := io.Copy(io.MultiWriter(destFile, hash), chunkFile)
		chunkFile.Close()

		if err != nil {
			return fmt.Errorf("合并分片失败: %w", err)
		}

		totalWritten += written
	}

	// 验证文件大小
	if totalWritten != data.FileSize {
		os.Remove(destPath)
		return fmt.Errorf("文件大小不匹配, 预期: %d, 实际: %d", data.FileSize, totalWritten)
	}

	// 验证文件哈希
	calculatedHash := hex.EncodeToString(hash.Sum(nil))
	if calculatedHash != data.FileHash {
		os.Remove(destPath)
		return fmt.Errorf("文件哈希不匹配, 预期: %s, 计算得: %s", data.FileHash, calculatedHash)
	}

	// 更新数据库中的文件状态
	if err := model.UpdateFileStatus(ms.dbService.DB, fileID, true); err != nil {
		log.Printf("更新文件状态失败: %v", err)
		// 不返回错误，因为文件已经合并成功
	}

	// 清理临时分片文件
	go func() {
		if err := os.RemoveAll(chunkDir); err != nil {
			log.Printf("清理临时分片失败: %v", err)
		}
	}()

	// 保存文件哈希到Redis，用于秒传
	if err := ms.redisService.SaveFileHash(ctx, data.FileHash, fileID); err != nil {
		log.Printf("保存文件哈希到Redis失败: %v", err)
	}

	log.Printf("文件合并完成: %s, 文件ID: %s, 大小: %d, 哈希: %s",
		data.FileName, fileID, totalWritten, calculatedHash)

	return nil
}

// PublishMergeTask 发布合并任务
func (ms *MergeService) PublishMergeTask(ctx context.Context, fileID string, userID int, fileName string, fileSize int64, fileHash string, chunkCount int) error {
	mergeData := &MergeTaskData{
		UserID:     userID,
		FileHash:   fileHash,
		FileName:   fileName,
		FileSize:   fileSize,
		ChunkCount: chunkCount,
		TempDir:    ms.tempDir,
		DestDir:    ms.uploadDir,
	}

	return ms.mqService.PublishFileMergeTask(ctx, fileID, mergeData)
}
