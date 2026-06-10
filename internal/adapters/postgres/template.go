package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/turgut1907/notification.system/internal/domain"
)

// GetTemplateByID loads a template, decoding its required_variables JSONB.
func (s *Store) GetTemplateByID(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	var t domain.Template
	var requiredVars []byte
	err := s.reader.QueryRow(ctx,
		`SELECT id, name, channel, content, required_variables, created_at
		 FROM templates WHERE id = $1`, id).
		Scan(&t.ID, &t.Name, &t.Channel, &t.Content, &requiredVars, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Template{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Template{}, fmt.Errorf("get template: %w", err)
	}

	if len(requiredVars) > 0 {
		if err := json.Unmarshal(requiredVars, &t.RequiredVariables); err != nil {
			return domain.Template{}, fmt.Errorf("decode required_variables: %w", err)
		}
	}
	return t, nil
}
