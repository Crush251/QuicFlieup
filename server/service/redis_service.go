package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisService Redis服务
type RedisService struct {
	client *redis.Client
}

// 常量定义
const (
	FileHashKeyPrefix    = "file:hash:"        // 文件哈希前缀，用于秒传: file:hash:{filehash} -> fileID
	FileChunksKeyPrefix  = "file:chunks:"      // 文件分片前缀: file:chunks:{fileID} -> Set of chunk numbers
	FileInfoKeyPrefix    = "file:info:"        // 文件信息前缀: file:info:{fileID} -> JSON string of file info
	UserFilesKeyPrefix   = "user:files:"       // 用户文件前缀: user:files:{userID} -> Set of fileIDs
	FileUploadLockPrefix = "file:upload:lock:" // 文件上传锁前缀: file:upload:lock:{fileID} -> 1 (with expiry)
	FileExpiry           = 24 * time.Hour      // 文件信息过期时间
	UploadLockExpiry     = 30 * time.Minute    // 上传锁过期时间
)

// NewRedisService 创建新的Redis服务
func NewRedisService(addr string, password string, db int) *RedisService {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &RedisService{
		client: client,
	}
}

// Close 关闭Redis连接
func (rs *RedisService) Close() error {
	return rs.client.Close()
}

// SaveFileHash 保存文件哈希到文件ID的映射，用于秒传
func (rs *RedisService) SaveFileHash(ctx context.Context, fileHash, fileID string) error {
	return rs.client.Set(ctx, FileHashKeyPrefix+fileHash, fileID, FileExpiry).Err()
}

// GetFileIDByHash 通过文件哈希获取文件ID，用于秒传
func (rs *RedisService) GetFileIDByHash(ctx context.Context, fileHash string) (string, error) {
	return rs.client.Get(ctx, FileHashKeyPrefix+fileHash).Result()
}

// 保存文件信息
func (rs *RedisService) SaveFileInfo(ctx context.Context, fileID string, fileInfo interface{}) error {
	data, err := json.Marshal(fileInfo)
	if err != nil {
		return err
	}
	return rs.client.Set(ctx, FileInfoKeyPrefix+fileID, data, FileExpiry).Err()
}

// 获取文件信息
func (rs *RedisService) GetFileInfo(ctx context.Context, fileID string, dst interface{}) error {
	data, err := rs.client.Get(ctx, FileInfoKeyPrefix+fileID).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(data), dst)
}

// 添加已上传的分片
func (rs *RedisService) AddUploadedChunk(ctx context.Context, fileID string, chunkNum int) error {
	key := FileChunksKeyPrefix + fileID
	// 添加分片编号到集合
	err := rs.client.SAdd(ctx, key, chunkNum).Err()
	if err != nil {
		return err
	}
	// 设置过期时间
	return rs.client.Expire(ctx, key, FileExpiry).Err()
}

// 获取已上传的分片列表
func (rs *RedisService) GetUploadedChunks(ctx context.Context, fileID string) ([]int, error) {
	key := FileChunksKeyPrefix + fileID
	result, err := rs.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	chunks := make([]int, 0, len(result))
	for _, v := range result {
		chunkNum, err := strconv.Atoi(v)
		if err != nil {
			continue
		}
		chunks = append(chunks, chunkNum)
	}
	return chunks, nil
}

// 检查分片是否已上传
func (rs *RedisService) IsChunkUploaded(ctx context.Context, fileID string, chunkNum int) (bool, error) {
	key := FileChunksKeyPrefix + fileID
	return rs.client.SIsMember(ctx, key, chunkNum).Result()
}

// 添加文件到用户的文件列表
func (rs *RedisService) AddFileToUser(ctx context.Context, userID int, fileID string) error {
	key := fmt.Sprintf("%s%d", UserFilesKeyPrefix, userID)
	err := rs.client.SAdd(ctx, key, fileID).Err()
	if err != nil {
		return err
	}
	return rs.client.Expire(ctx, key, FileExpiry).Err()
}

// 获取用户的文件ID列表
func (rs *RedisService) GetUserFiles(ctx context.Context, userID int) ([]string, error) {
	key := fmt.Sprintf("%s%d", UserFilesKeyPrefix, userID)
	return rs.client.SMembers(ctx, key).Result()
}

// 尝试获取文件上传锁
func (rs *RedisService) AcquireUploadLock(ctx context.Context, fileID string) (bool, error) {
	key := FileUploadLockPrefix + fileID
	return rs.client.SetNX(ctx, key, 1, UploadLockExpiry).Result()
}

// 释放文件上传锁
func (rs *RedisService) ReleaseUploadLock(ctx context.Context, fileID string) error {
	key := FileUploadLockPrefix + fileID
	return rs.client.Del(ctx, key).Err()
}

// 清除文件相关的所有Redis记录
func (rs *RedisService) ClearFileData(ctx context.Context, fileID string, fileHash string) error {
	pipeline := rs.client.Pipeline()

	// 删除文件信息
	pipeline.Del(ctx, FileInfoKeyPrefix+fileID)
	// 删除分片记录
	pipeline.Del(ctx, FileChunksKeyPrefix+fileID)
	// 删除哈希映射
	if fileHash != "" {
		pipeline.Del(ctx, FileHashKeyPrefix+fileHash)
	}
	// 删除上传锁
	pipeline.Del(ctx, FileUploadLockPrefix+fileID)

	_, err := pipeline.Exec(ctx)
	return err
}
