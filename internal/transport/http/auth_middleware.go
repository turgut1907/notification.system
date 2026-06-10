package httpapi

import (
	"net/http"
	"strings"

	"github.com/turgut1907/notification.system/internal/platform/auth"
)

const authHeader = "Authorization"

func authMiddleware(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !requiresAuth(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			token := bearerToken(r.Header.Get(authHeader))
			if token == "" {
				writeJSON(w, http.StatusUnauthorized, errorResponse{
					Error: "unauthorized", Message: "missing or invalid Authorization header",
				})
				return
			}

			userID, err := auth.ParseToken(secret, token)
			if err != nil {
				writeJSON(w, http.StatusUnauthorized, errorResponse{
					Error: "unauthorized", Message: "invalid or expired token",
				})
				return
			}

			next.ServeHTTP(w, r.WithContext(auth.WithUserID(r.Context(), userID)))
		})
	}
}

func requiresAuth(path string) bool {
	return strings.HasPrefix(path, "/api/v1/")
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(header, prefix))
}
