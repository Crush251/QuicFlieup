package handler

import (
	"QuicFlieup/server/model"
	"QuicFlieup/server/service"
	"QuicFlieup/server/utils"
	"encoding/json"
	"net/http"
	"time"
)

// UserHandler 用户处理器
type UserHandler struct {
	dbService *service.DBService
}

// NewUserHandler 创建用户处理器
func NewUserHandler(dbService *service.DBService) *UserHandler {
	return &UserHandler{
		dbService: dbService,
	}
}

// Register 用户注册处理
func (h *UserHandler) Register(w http.ResponseWriter, r *http.Request) {
	// 只接受POST请求
	if r.Method != http.MethodPost {
		utils.JSONResponse(w, http.StatusMethodNotAllowed, map[string]interface{}{
			"success": false,
			"message": "只支持POST请求",
		})
		return
	}

	// 解析请求体
	var req model.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "无效的请求数据",
		})
		return
	}

	// 验证请求数据
	if req.Username == "" || req.Password == "" || req.Email == "" {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "用户名、密码和邮箱不能为空",
		})
		return
	}

	// 创建用户
	user, err := model.CreateUser(h.dbService.DB, req)
	if err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "注册失败: " + err.Error(),
		})
		return
	}

	// 生成JWT令牌
	token, err := utils.GenerateJWT(user.ID, user.Username)
	if err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "生成令牌失败",
		})
		return
	}

	// 设置Cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(time.Hour * 24 * 7 / time.Second), // 7天有效期
	})

	// 返回成功结果
	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "用户注册成功",
		"data": map[string]interface{}{
			"user":  user,
			"token": token,
		},
	})
}

// Login 用户登录处理
func (h *UserHandler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		utils.JSONResponse(w, http.StatusMethodNotAllowed, map[string]interface{}{
			"success": false,
			"message": "只支持POST请求",
		})
		return
	}

	// 解析请求体
	var req model.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		utils.JSONResponse(w, http.StatusBadRequest, map[string]interface{}{
			"success": false,
			"message": "无效的请求数据",
		})
		return
	}

	// 验证用户凭据
	user, err := model.AuthenticateUser(h.dbService.DB, req)
	if err != nil {
		utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"message": "登录失败: " + err.Error(),
		})
		return
	}

	// 生成JWT令牌
	token, err := utils.GenerateJWT(user.ID, user.Username)
	if err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "生成令牌失败",
		})
		return
	}

	// 设置Cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(time.Hour * 24 * 7 / time.Second), // 7天有效期
	})

	// 返回成功结果
	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "登录成功",
		"data": map[string]interface{}{
			"user":  user,
			"token": token,
		},
	})
}

// GetUserInfo 获取用户信息
func (h *UserHandler) GetUserInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		utils.JSONResponse(w, http.StatusMethodNotAllowed, map[string]interface{}{
			"success": false,
			"message": "只支持GET请求",
		})
		return
	}

	// 从上下文中获取用户信息
	userID, ok := r.Context().Value("userID").(int)
	if !ok {
		utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"message": "未授权",
		})
		return
	}

	// 获取用户信息
	user, err := model.GetUserByID(h.dbService.DB, userID)
	if err != nil {
		utils.JSONResponse(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false,
			"message": "获取用户信息失败: " + err.Error(),
		})
		return
	}

	// 返回用户信息
	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"data":    user,
	})
}

// Logout 用户退出登录
func (h *UserHandler) Logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		utils.JSONResponse(w, http.StatusMethodNotAllowed, map[string]interface{}{
			"success": false,
			"message": "只支持POST请求",
		})
		return
	}

	// 清除Cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1, // 立即过期
	})

	utils.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "退出登录成功",
	})
}
