package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	if fileHash == "" || fileID == "" {
		log.Printf("错误：保存文件哈希时参数无效，哈希=%s, 文件ID=%s", fileHash, fileID)
		return fmt.Errorf("保存文件哈希的参数无效")
	}

	key := FileHashKeyPrefix + fileHash
	log.Printf("保存文件哈希映射: key=%s, 哈希=%s -> 文件ID=%s", key, fileHash, fileID)

	// 先检查这个哈希是否已经存在
	existingID, err := rs.client.Get(ctx, key).Result()
	if err == nil {
		log.Printf("文件哈希已存在: %s -> %s, 将更新为新ID: %s", fileHash, existingID, fileID)
	} else if err != redis.Nil {
		log.Printf("检查现有哈希时出错: %v", err)
	}

	err = rs.client.Set(ctx, key, fileID, FileExpiry).Err()
	if err != nil {
		log.Printf("保存文件哈希映射失败: %v", err)
		return err
	}

	// 再次验证设置是否成功
	savedID, err := rs.client.Get(ctx, key).Result()
	if err != nil {
		log.Printf("验证保存结果失败: %v", err)
	} else if savedID != fileID {
		log.Printf("警告：保存的文件ID与预期不符: 期望=%s, 实际=%s", fileID, savedID)
	} else {
		log.Printf("文件哈希映射保存成功并已验证: %s -> %s", fileHash, fileID)
	}

	// 检查键的过期时间
	ttl, err := rs.client.TTL(ctx, key).Result()
	if err != nil {
		log.Printf("检查键过期时间失败: %v", err)
	} else {
		log.Printf("哈希键设置的过期时间: %s", ttl.String())
	}

	return nil
}

// GetFileIDByHash 通过文件哈希获取文件ID，用于秒传
func (rs *RedisService) GetFileIDByHash(ctx context.Context, fileHash string) (string, error) {
	if fileHash == "" {
		log.Printf("错误：查询文件哈希时参数无效")
		return "", fmt.Errorf("哈希参数为空")
	}

	key := FileHashKeyPrefix + fileHash
	log.Printf("查询文件哈希: key=%s", key)

	fileID, err := rs.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			log.Printf("文件哈希不存在: %s", fileHash)
		} else {
			log.Printf("获取文件哈希映射失败: %v", err)
		}
		return "", err
	}

	// 每次访问时刷新过期时间
	if err := rs.client.Expire(ctx, key, FileExpiry).Err(); err != nil {
		log.Printf("刷新哈希键过期时间失败: %v", err)
	}

	log.Printf("文件哈希查询成功: 哈希=%s -> 文件ID=%s", fileHash, fileID)
	return fileID, nil
}

// CheckFileHashExists 检查文件哈希是否存在，用于调试
func (rs *RedisService) CheckFileHashExists(ctx context.Context, fileHash string) (bool, string, error) {
	key := FileHashKeyPrefix + fileHash
	log.Printf("检查文件哈希是否存在: %s", key)

	fileID, err := rs.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			log.Printf("文件哈希不存在: %s", fileHash)
			return false, "", nil
		}
		log.Printf("检查文件哈希时出错: %v", err)
		return false, "", err
	}

	ttl, _ := rs.client.TTL(ctx, key).Result()
	log.Printf("文件哈希存在: %s -> %s, TTL=%s", fileHash, fileID, ttl.String())
	return true, fileID, nil
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
		log.Printf("添加分片到Redis失败: 文件ID=%s, 分片=%d, 错误=%v", fileID, chunkNum, err)
		return err
	}
	// 设置过期时间
	err = rs.client.Expire(ctx, key, FileExpiry).Err()
	if err != nil {
		log.Printf("设置分片记录过期时间失败: %v", err)
	} else {
		log.Printf("分片添加成功: 文件ID=%s, 分片=%d", fileID, chunkNum)
	}
	return err
}

// 获取已上传的分片列表
func (rs *RedisService) GetUploadedChunks(ctx context.Context, fileID string) ([]int, error) {
	key := FileChunksKeyPrefix + fileID
	log.Printf("获取已上传分片: 文件ID=%s", fileID)
	result, err := rs.client.SMembers(ctx, key).Result()
	if err != nil {
		log.Printf("获取分片列表失败: %v", err)
		return nil, err
	}

	chunks := make([]int, 0, len(result))
	for _, v := range result {
		chunkNum, err := strconv.Atoi(v)
		if err != nil {
			log.Printf("转换分片编号失败: %s, %v", v, err)
			continue
		}
		chunks = append(chunks, chunkNum)
	}
	log.Printf("已上传分片: 文件ID=%s, 数量=%d", fileID, len(chunks))
	return chunks, nil
}

// 检查分片是否已上传
func (rs *RedisService) IsChunkUploaded(ctx context.Context, fileID string, chunkNum int) (bool, error) {
	key := FileChunksKeyPrefix + fileID
	exists, err := rs.client.SIsMember(ctx, key, chunkNum).Result()
	if err != nil {
		log.Printf("检查分片是否存在失败: 文件ID=%s, 分片=%d, 错误=%v", fileID, chunkNum, err)
		return false, err
	}
	if exists {
		log.Printf("分片已存在: 文件ID=%s, 分片=%d", fileID, chunkNum)
	}
	return exists, nil
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

// Client 返回Redis客户端对象，用于调试和特殊操作
func (rs *RedisService) Client() *redis.Client {
	return rs.client
}
