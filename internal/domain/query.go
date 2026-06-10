package domain

import (
	"time"

	"github.com/google/uuid"
)

// ListFilter describes filtering and keyset pagination for listing notifications.
// Cursor is the ID of the last item from the previous page (created_at, id ordering).
type ListFilter struct {
	UserID   string
	Status   *RequestStatus
	Channel  *Channel
	From     *time.Time
	To       *time.Time
	Limit    int
	Cursor   *uuid.UUID
	CursorAt *time.Time
}

// Normalize clamps the limit into a sane range.
func (f *ListFilter) Normalize() {
	const (
		defaultLimit = 50
		maxLimit     = 200
	)
	if f.Limit <= 0 {
		f.Limit = defaultLimit
	}
	if f.Limit > maxLimit {
		f.Limit = maxLimit
	}
}

// Page is a page of requests plus the cursor to fetch the next page.
type Page struct {
	Items      []Request
	NextCursor *uuid.UUID
	NextAt     *time.Time
}
