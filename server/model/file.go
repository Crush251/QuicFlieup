package model

import (
	"database/sql"
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
	file := &FileInfo{}
	err := db.QueryRow(
		`SELECT id, file_id, user_id, file_name, file_size, file_hash, 
		chunk_count, is_completed, created_at, updated_at 
		FROM files WHERE file_hash = ? AND is_completed = 1 LIMIT 1`,
		fileHash,
	).Scan(
		&file.ID, &file.FileID, &file.UserID, &file.FileName, &file.FileSize,
		&file.FileHash, &file.ChunkCount, &file.IsCompleted, &file.CreatedAt, &file.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // 文件不存在，返回nil而不是错误
		}
		return nil, err
	}

	return file, nil
}

// CreateFile 创建文件记录
func CreateFile(db *sql.DB, file *FileInfo) error {
	_, err := db.Exec(
		`INSERT INTO files 
		(file_id, user_id, file_name, file_size, file_hash, chunk_count, is_completed) 
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		file.FileID, file.UserID, file.FileName, file.FileSize, file.FileHash, file.ChunkCount, file.IsCompleted,
	)
	return err
}

// UpdateFileStatus 更新文件状态
func UpdateFileStatus(db *sql.DB, fileID string, isCompleted bool) error {
	_, err := db.Exec(
		"UPDATE files SET is_completed = ? WHERE file_id = ?",
		isCompleted, fileID,
	)
	return err
}

// GetFileByID 通过文件ID获取文件信息
func GetFileByID(db *sql.DB, fileID string) (*FileInfo, error) {
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
			return nil, nil
		}
		return nil, err
	}

	return file, nil
}

// GetUserFiles 获取用户的所有文件
func GetUserFiles(db *sql.DB, userID int) ([]*FileInfo, error) {
	rows, err := db.Query(
		`SELECT id, file_id, user_id, file_name, file_size, file_hash, 
		chunk_count, is_completed, created_at, updated_at 
		FROM files WHERE user_id = ? ORDER BY created_at DESC`,
		userID,
	)
	if err != nil {
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
			return nil, err
		}
		files = append(files, file)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return files, nil
}
