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
	"time"

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
	log.Printf("开始合并文件任务: ID=%s, 文件名=%s, 大小=%.2f MB, 分片数=%d, 哈希值=%s",
		fileID, data.FileName, float64(data.FileSize)/(1024*1024), data.ChunkCount, data.FileHash)

	// 创建目标文件夹确保存在
	if err := os.MkdirAll(ms.uploadDir, 0755); err != nil {
		log.Printf("创建上传目录失败: %v", err)
		return fmt.Errorf("创建上传目录失败: %w", err)
	}

	// 创建目标文件
	destPath := filepath.Join(ms.uploadDir, data.FileName)
	log.Printf("目标文件路径: %s", destPath)

	destFile, err := os.Create(destPath)
	if err != nil {
		log.Printf("创建目标文件失败: %v", err)
		return fmt.Errorf("创建目标文件失败: %w", err)
	}
	defer destFile.Close()

	// 创建MD5哈希计算器
	hash := md5.New()

	// 合并所有分片
	chunkDir := filepath.Join(ms.tempDir, fileID)
	var totalWritten int64
	var lastProgressReport time.Time = time.Now()

	// 检查临时目录是否存在
	if _, err := os.Stat(chunkDir); os.IsNotExist(err) {
		log.Printf("分片目录不存在: %s", chunkDir)
		return fmt.Errorf("分片目录不存在: %s", chunkDir)
	} else {
		log.Printf("找到分片目录: %s, 开始合并", chunkDir)
	}

	// 实时报告进度
	reportProgress := func(i int, written int64) {
		if time.Since(lastProgressReport) > 5*time.Second {
			percentage := float64(i+1) / float64(data.ChunkCount) * 100
			writtenMB := float64(written) / (1024 * 1024)
			totalMB := float64(data.FileSize) / (1024 * 1024)
			log.Printf("合并进度: %.2f%% (分片 %d/%d), 已写入: %.2f MB / %.2f MB",
				percentage, i+1, data.ChunkCount, writtenMB, totalMB)
			lastProgressReport = time.Now()
		}
	}

	for i := 0; i < data.ChunkCount; i++ {
		chunkPath := filepath.Join(chunkDir, strconv.Itoa(i))
		log.Printf("处理分片 %d/%d: %s", i+1, data.ChunkCount, chunkPath)

		// 检查分片是否存在
		if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
			log.Printf("分片文件不存在: %s (序号: %d/%d)", chunkPath, i+1, data.ChunkCount)
			os.Remove(destPath) // 删除不完整的文件
			return fmt.Errorf("分片文件不存在: %s (序号: %d/%d)", chunkPath, i+1, data.ChunkCount)
		}

		// 打开分片文件
		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			log.Printf("打开分片文件失败: %v (序号: %d/%d)", err, i+1, data.ChunkCount)
			os.Remove(destPath) // 删除不完整的文件
			return fmt.Errorf("打开分片文件失败: %w (序号: %d/%d)", err, i+1, data.ChunkCount)
		}

		// 写入目标文件并计算哈希
		written, err := io.Copy(io.MultiWriter(destFile, hash), chunkFile)
		chunkFile.Close()

		if err != nil {
			log.Printf("合并分片失败: %v (序号: %d/%d)", err, i+1, data.ChunkCount)
			os.Remove(destPath) // 删除不完整的文件
			return fmt.Errorf("合并分片失败: %w (序号: %d/%d)", err, i+1, data.ChunkCount)
		}

		totalWritten += written
		reportProgress(i, totalWritten)
	}

	// 确保所有数据都写入磁盘
	if err := destFile.Sync(); err != nil {
		log.Printf("同步文件到磁盘失败: %v", err)
		// 继续执行，不返回错误
	}

	// 验证文件大小
	if totalWritten != data.FileSize {
		log.Printf("文件大小不匹配, 预期: %d, 实际: %d", data.FileSize, totalWritten)
		os.Remove(destPath) // 删除不完整的文件
		return fmt.Errorf("文件大小不匹配, 预期: %d, 实际: %d", data.FileSize, totalWritten)
	}

	// 计算并验证文件哈希
	calculatedHash := hex.EncodeToString(hash.Sum(nil))
	log.Printf("文件哈希计算完成: 期望=%s, 实际=%s", data.FileHash, calculatedHash)

	// 对于大文件，我们可能不验证哈希
	skipHashVerify := false
	if data.FileSize > 1024*1024*1024 { // 大于1GB的文件
		skipHashVerify = true
		log.Printf("文件大小超过1GB，跳过哈希验证")
	}

	if !skipHashVerify && calculatedHash != data.FileHash {
		log.Printf("文件哈希不匹配, 预期: %s, 计算得: %s", data.FileHash, calculatedHash)
		os.Remove(destPath) // 删除不匹配的文件
		return fmt.Errorf("文件哈希不匹配, 预期: %s, 计算得: %s", data.FileHash, calculatedHash)
	}

	// 更新数据库中的文件状态
	if err := model.UpdateFileStatus(ms.dbService.DB, fileID, true); err != nil {
		log.Printf("更新文件状态失败: %v", err)
		// 不返回错误，因为文件已经合并成功
	} else {
		log.Printf("数据库文件状态更新成功: %s", fileID)
	}

	// 保存文件哈希到Redis，用于秒传
	// 对于大文件，使用计算出的哈希
	hashToSave := data.FileHash
	if skipHashVerify {
		hashToSave = calculatedHash
	}

	// 确保无论如何都保存哈希值
	if err := ms.redisService.SaveFileHash(ctx, hashToSave, fileID); err != nil {
		log.Printf("保存文件哈希到Redis失败: %v", err)
	} else {
		log.Printf("文件哈希保存到Redis成功: %s -> %s", hashToSave, fileID)
	}

	// 清理临时分片文件
	go func() {
		if err := os.RemoveAll(chunkDir); err != nil {
			log.Printf("清理临时分片失败: %v", err)
		} else {
			log.Printf("临时分片文件清理成功: %s", chunkDir)
		}
	}()

	log.Printf("文件合并完成: 文件名=%s, 文件ID=%s, 大小=%.2f MB, 哈希=%s, 位置=%s",
		data.FileName, fileID, float64(totalWritten)/(1024*1024), hashToSave, destPath)

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
