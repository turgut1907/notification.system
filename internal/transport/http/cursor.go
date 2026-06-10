package httpapi

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// encodeCursor packs (created_at, id) into an opaque, URL-safe pagination token.
func encodeCursor(id uuid.UUID, at *time.Time) string {
	var ts int64
	if at != nil {
		ts = at.UnixNano()
	}
	raw := strconv.FormatInt(ts, 10) + "|" + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor reverses encodeCursor.
func decodeCursor(s string) (uuid.UUID, *time.Time, error) {
	data, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return uuid.Nil, nil, err
	}
	parts := strings.SplitN(string(data), "|", 2)
	if len(parts) != 2 {
		return uuid.Nil, nil, errors.New("malformed cursor")
	}
	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return uuid.Nil, nil, err
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return uuid.Nil, nil, err
	}
	at := time.Unix(0, ts).UTC()
	return id, &at, nil
}

// parseInt parses a base-10 integer.
func parseInt(s string) (int, error) {
	return strconv.Atoi(s)
}

// parseQueryTime parses an RFC3339 timestamp from a query parameter.
func parseQueryTime(s string) (time.Time, error) {
	return time.Parse(time.RFC3339, s)
}
