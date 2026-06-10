package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/turgut1907/notification.system/internal/platform/auth"
)

const testJWTSecret = "01234567890123456789012345678901"

func TestGentokenPrintsValidJWT(t *testing.T) {
	if os.Getenv("JWT_SECRET") == "" {
		t.Setenv("JWT_SECRET", testJWTSecret)
	}

	bin := filepath.Join(t.TempDir(), "gentoken")
	out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput()
	if err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	cmd := exec.Command(bin, "demo-user")
	cmd.Env = append(os.Environ(), "JWT_SECRET="+testJWTSecret)
	tokenBytes, err := cmd.Output()
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	token := strings.TrimSpace(string(tokenBytes))
	userID, err := auth.ParseToken(testJWTSecret, token)
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	if userID != "demo-user" {
		t.Fatalf("user_id %q", userID)
	}
}

func TestGentokenRequiresUserIDArg(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "gentoken")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "JWT_SECRET="+testJWTSecret)
	if err := cmd.Run(); err == nil {
		t.Fatal("expected error when user_id arg missing")
	}
}
