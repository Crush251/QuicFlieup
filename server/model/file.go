package model

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

// FileInfo 文件信息结构体
type FileInfo struct {
	ID          int       `json:"id"`
	FileID      string    `json:"fileId"`
	UserID      int       `json:"userId"`
	FileName    string    `json:"fileName"`
	FileSize    int64     `json:"fileSize"`
	FileHash    string    `json:"fileHash"`
	ChunkCount  int       `json:"chunkCount"`
	IsCompleted bool      `json:"isCompleted"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// GetFileByHash 通过文件哈希获取文件信息
func GetFileByHash(db *sql.DB, fileHash string) (*FileInfo, error) {
	if fileHash == "" {
		log.Printf("错误：哈希参数为空")
		return nil, fmt.Errorf("哈希参数为空")
	}

	log.Printf("通过哈希查询文件: %s", fileHash)

	// 首先检查表是否存在
	var tableExists int
	err := db.QueryRow("SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'files' LIMIT 1").Scan(&tableExists)

	if err != nil {
		log.Printf("检查表是否存在失败: %v", err)
		// 继续执行，不阻断流程
	} else if tableExists != 1 {
		log.Printf("文件表不存在，可能需要初始化数据库")
		return nil, nil
	}

	file := &FileInfo{}
	query := `SELECT id, file_id, user_id, file_name, file_size, file_hash, 
		chunk_count, is_completed, created_at, updated_at 
		FROM files WHERE file_hash = ? AND is_completed = 1 LIMIT 1`

	log.Printf("执行SQL查询: %s, 参数: %s", query, fileHash)

	err = db.QueryRow(query, fileHash).Scan(
		&file.ID, &file.FileID, &file.UserID, &file.FileName, &file.FileSize,
		&file.FileHash, &file.ChunkCount, &file.IsCompleted, &file.CreatedAt, &file.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("未找到匹配哈希的文件: %s", fileHash)
			return nil, nil // 文件不存在，返回nil而不是错误
		}
		log.Printf("查询文件哈希失败: %v", err)
		return nil, err
	}

	log.Printf("找到匹配哈希的文件: %s, 文件ID=%s, 文件名=%s, 大小=%.2f MB, 完成状态=%v",
		fileHash, file.FileID, file.FileName, float64(file.FileSize)/(1024*1024), file.IsCompleted)
	return file, nil
}

// CreateFile 创建文件记录
func CreateFile(db *sql.DB, file *FileInfo) error {
	log.Printf("创建文件记录: ID=%s, 用户=%d, 文件名=%s, 大小=%d",
		file.FileID, file.UserID, file.FileName, file.FileSize)

	result, err := db.Exec(
		`INSERT INTO files 
		(file_id, user_id, file_name, file_size, file_hash, chunk_count, is_completed) 
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		file.FileID, file.UserID, file.FileName, file.FileSize, file.FileHash, file.ChunkCount, file.IsCompleted,
	)

	if err != nil {
		log.Printf("创建文件记录失败: %v", err)
		return err
	}

	id, _ := result.LastInsertId()
	log.Printf("文件记录创建成功: 数据库ID=%d", id)
	return nil
}

// UpdateFileStatus 更新文件状态
func UpdateFileStatus(db *sql.DB, fileID string, isCompleted bool) error {
	log.Printf("更新文件状态: ID=%s, 完成状态=%v", fileID, isCompleted)

	result, err := db.Exec(
		"UPDATE files SET is_completed = ? WHERE file_id = ?",
		isCompleted, fileID,
	)

	if err != nil {
		log.Printf("更新文件状态失败: %v", err)
		return err
	}

	rows, _ := result.RowsAffected()
	log.Printf("文件状态更新成功: 影响行数=%d", rows)
	return nil
}

// GetFileByID 通过文件ID获取文件信息
func GetFileByID(db *sql.DB, fileID string) (*FileInfo, error) {
	log.Printf("通过ID查询文件: %s", fileID)

	file := &FileInfo{}
	err := db.QueryRow(
		`SELECT id, file_id, user_id, file_name, file_size, file_hash, 
		chunk_count, is_completed, created_at, updated_at 
		FROM files WHERE file_id = ?`,
		fileID,
	).Scan(
		&file.ID, &file.FileID, &file.UserID, &file.FileName, &file.FileSize,
		&file.FileHash, &file.ChunkCount, &file.IsCompleted, &file.CreatedAt, &file.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("未找到ID对应的文件: %s", fileID)
			return nil, nil
		}
		log.Printf("查询文件ID失败: %v", err)
		return nil, err
	}

	log.Printf("找到ID对应的文件: %s, 文件名=%s, 大小=%d", fileID, file.FileName, file.FileSize)
	return file, nil
}

// GetUserFiles 获取用户的所有文件
func GetUserFiles(db *sql.DB, userID int) ([]*FileInfo, error) {
	log.Printf("获取用户文件列表: 用户ID=%d", userID)

	rows, err := db.Query(
		`SELECT id, file_id, user_id, file_name, file_size, file_hash, 
		chunk_count, is_completed, created_at, updated_at 
		FROM files WHERE user_id = ? ORDER BY created_at DESC`,
		userID,
	)
	if err != nil {
		log.Printf("查询用户文件列表失败: %v", err)
		return nil, err
	}
	defer rows.Close()

	var files []*FileInfo
	for rows.Next() {
		file := &FileInfo{}
		err := rows.Scan(
			&file.ID, &file.FileID, &file.UserID, &file.FileName, &file.FileSize,
			&file.FileHash, &file.ChunkCount, &file.IsCompleted, &file.CreatedAt, &file.UpdatedAt,
		)
		if err != nil {
			log.Printf("扫描文件行失败: %v", err)
			return nil, err
		}
		files = append(files, file)
	}

	if err = rows.Err(); err != nil {
		log.Printf("遍历文件行失败: %v", err)
		return nil, err
	}

	log.Printf("用户文件列表获取成功: 用户ID=%d, 文件数=%d", userID, len(files))
	return files, nil
}
