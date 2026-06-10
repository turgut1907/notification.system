// Command gentoken prints a long-lived demo JWT for local API testing.
package main

import (
	"fmt"
	"os"

	"github.com/turgut1907/notification.system/internal/platform/auth"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: gentoken <user_id>")
		os.Exit(2)
	}

	secret := os.Getenv("JWT_SECRET")
	if err := auth.ValidateSecret(secret); err != nil {
		fmt.Fprintf(os.Stderr, "gentoken: %v\n", err)
		os.Exit(1)
	}

	token, err := auth.SignToken(secret, os.Args[1], auth.DemoTokenTTL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gentoken: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(token)
}
