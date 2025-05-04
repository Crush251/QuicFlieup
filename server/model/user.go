package model

import (
	"database/sql"
	"errors"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// User 结构体定义
type User struct {
	ID           int       `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"` // 不输出到JSON
	Email        string    `json:"email"`
	CreatedAt    time.Time `json:"createdAt"`
}

// 用户注册请求
type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Email    string `json:"email"`
}

// 用户登录请求
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// CreateUser 创建新用户
func CreateUser(db *sql.DB, req RegisterRequest) (*User, error) {
	// 检查用户名是否已存在
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM users WHERE username = ?", req.Username).Scan(&count)
	if err != nil {
		return nil, err
	}
	if count > 0 {
		return nil, errors.New("用户名已存在")
	}

	// 检查邮箱是否已存在
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE email = ?", req.Email).Scan(&count)
	if err != nil {
		return nil, err
	}
	if count > 0 {
		return nil, errors.New("邮箱已被注册")
	}

	// 密码加密
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	// 插入用户记录
	result, err := db.Exec(
		"INSERT INTO users (username, password_hash, email) VALUES (?, ?, ?)",
		req.Username, string(hashedPassword), req.Email,
	)
	if err != nil {
		return nil, err
	}

	// 获取用户ID
	userID, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	// 返回新创建的用户
	return &User{
		ID:           int(userID),
		Username:     req.Username,
		PasswordHash: string(hashedPassword),
		Email:        req.Email,
		CreatedAt:    time.Now(),
	}, nil
}

// GetUserByUsername 通过用户名获取用户
func GetUserByUsername(db *sql.DB, username string) (*User, error) {
	user := &User{}
	err := db.QueryRow(
		"SELECT id, username, password_hash, email, created_at FROM users WHERE username = ?",
		username,
	).Scan(&user.ID, &user.Username, &user.PasswordHash, &user.Email, &user.CreatedAt)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New("用户不存在")
		}
		return nil, err
	}

	return user, nil
}

// AuthenticateUser 用户身份验证
func AuthenticateUser(db *sql.DB, req LoginRequest) (*User, error) {
	user, err := GetUserByUsername(db, req.Username)
	if err != nil {
		return nil, err
	}

	// 验证密码
	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password))
	if err != nil {
		return nil, errors.New("密码不正确")
	}

	return user, nil
}

// GetUserByID 通过ID获取用户
func GetUserByID(db *sql.DB, id int) (*User, error) {
	user := &User{}
	err := db.QueryRow(
		"SELECT id, username, password_hash, email, created_at FROM users WHERE id = ?",
		id,
	).Scan(&user.ID, &user.Username, &user.PasswordHash, &user.Email, &user.CreatedAt)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New("用户不存在")
		}
		return nil, err
	}

	return user, nil
}
