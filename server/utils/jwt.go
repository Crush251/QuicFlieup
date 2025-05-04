package utils

import (
	"errors"
	"net/http"
	"time"

	"github.com/dgrijalva/jwt-go"
)

// JWT密钥，实际应用中应该从配置或环境变量中获取
const jwtSecret = "quic-file-upload-secret-key"

// JWT过期时间，7天
const tokenExpireDuration = time.Hour * 24 * 7

// Claims JWT声明结构
type Claims struct {
	UserID   int    `json:"userId"`
	Username string `json:"username"`
	jwt.StandardClaims
}

// GenerateJWT 生成JWT令牌
func GenerateJWT(userID int, username string) (string, error) {
	// 创建一个我们自己的声明
	claims := Claims{
		UserID:   userID,
		Username: username,
		StandardClaims: jwt.StandardClaims{
			ExpiresAt: time.Now().Add(tokenExpireDuration).Unix(), // 过期时间
			IssuedAt:  time.Now().Unix(),                          // 签发时间
			Subject:   "user token",                               // 主题
			Issuer:    "quic-file-upload",                         // 签发人
		},
	}

	// 使用指定的签名方法创建签名对象
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// 使用指定的secret签名并获得完整的编码后的字符串token
	return token.SignedString([]byte(jwtSecret))
}

// ValidateJWT 验证JWT令牌并返回用户ID
func ValidateJWT(tokenString string) (int, error) {
	// 解析token
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(jwtSecret), nil
	})

	if err != nil {
		return 0, err
	}

	// 校验token
	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims.UserID, nil
	}

	return 0, errors.New("无效的令牌")
}

// GetClaimsFromToken 从令牌中获取用户信息
func GetClaimsFromToken(tokenString string) (*Claims, error) {
	// 解析token
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(jwtSecret), nil
	})

	if err != nil {
		return nil, err
	}

	// 校验token
	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims, nil
	}

	return nil, errors.New("无效的令牌")
}

// ExtractTokenFromRequest 从请求中提取令牌
func ExtractTokenFromRequest(r *http.Request) string {
	// 首先从Authorization头中尝试获取
	tokenString := r.Header.Get("Authorization")
	if tokenString != "" {
		// 如果格式为"Bearer {token}"，去除前缀
		if len(tokenString) > 7 && tokenString[:7] == "Bearer " {
			return tokenString[7:]
		}
		return tokenString
	}

	// 其次从Cookie中尝试获取
	cookie, err := r.Cookie("token")
	if err == nil && cookie.Value != "" {
		return cookie.Value
	}

	// 最后从URL查询参数中尝试获取
	return r.URL.Query().Get("token")
}
