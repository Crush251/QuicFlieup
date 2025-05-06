package middleware

import (
	"QuicFlieup/server/utils"
	"context"
	"net/http"
)

// AuthMiddleware 认证中间件，验证用户JWT令牌
func AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 从请求中提取令牌
		tokenString := utils.ExtractTokenFromRequest(r)
		if tokenString == "" {
			utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"message": "未提供身份验证令牌",
			})
			return
		}

		// 解析和验证令牌
		claims, err := utils.ParseToken(tokenString)
		if err != nil {
			utils.JSONResponse(w, http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"message": "无效的身份验证令牌: " + err.Error(),
			})
			return
		}

		// 将用户信息添加到请求上下文
		ctx := context.WithValue(r.Context(), "userID", claims.UserID)
		ctx = context.WithValue(ctx, "username", claims.Username)

		// 使用新的上下文继续处理请求
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// OptionalAuthMiddleware 可选认证中间件，不强制要求认证
func OptionalAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 从请求中提取令牌
		tokenString := utils.ExtractTokenFromRequest(r)
		if tokenString != "" {
			// 尝试解析令牌
			claims, err := utils.ParseToken(tokenString)
			if err == nil {
				// 如果令牌有效，将用户信息添加到请求上下文
				ctx := context.WithValue(r.Context(), "userID", claims.UserID)
				ctx = context.WithValue(ctx, "username", claims.Username)
				ctx = context.WithValue(ctx, "isAuthenticated", true)
				r = r.WithContext(ctx)
			}
		} else {
			// 未提供令牌，设置isAuthenticated为false
			ctx := context.WithValue(r.Context(), "isAuthenticated", false)
			r = r.WithContext(ctx)
		}

		// 继续处理请求
		next.ServeHTTP(w, r)
	})
}
