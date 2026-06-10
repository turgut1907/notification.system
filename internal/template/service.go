// Package template renders message templates with variable substitution and
// validates that all required variables are supplied. Rendering happens once at
// request-creation time; the rendered content is then immutable.
package template

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
)

// Repository is the port for loading templates.
type Repository interface {
	GetTemplateByID(ctx context.Context, id uuid.UUID) (domain.Template, error)
}

// placeholderRe matches {{ variable }} with optional surrounding whitespace.
var placeholderRe = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_]+)\s*\}\}`)

// Service renders and validates templates.
type Service struct {
	repo Repository
}

// New constructs a template Service.
func New(repo Repository) *Service {
	return &Service{repo: repo}
}

// Render loads the template, ensures its channel matches the notification channel,
// validates that all required variables are present, and returns the fully-substituted content.
func (s *Service) Render(ctx context.Context, templateID uuid.UUID, channel domain.Channel, variables map[string]string) (domain.Template, string, error) {
	tmpl, err := s.repo.GetTemplateByID(ctx, templateID)
	if err != nil {
		return domain.Template{}, "", err
	}

	if tmpl.Channel != channel {
		return domain.Template{}, "", domain.NewValidationError("channel",
			fmt.Sprintf("template channel %q does not match notification channel %q", tmpl.Channel, channel))
	}

	if missing := MissingVariables(tmpl.RequiredVariables, variables); len(missing) > 0 {
		return domain.Template{}, "", fmt.Errorf("%w: %s", domain.ErrTemplateVariablesMissing, strings.Join(missing, ", "))
	}

	return tmpl, RenderContent(tmpl.Content, variables), nil
}

// MissingVariables returns the sorted set of required variables absent from or
// blank in the supplied variables map.
func MissingVariables(required []string, variables map[string]string) []string {
	var missing []string
	for _, key := range required {
		if v, ok := variables[key]; !ok || strings.TrimSpace(v) == "" {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	return missing
}

// RenderContent substitutes {{var}} placeholders with their values. Unknown
// placeholders are left untouched so partial templates fail validation earlier
// rather than silently producing empty text.
func RenderContent(content string, variables map[string]string) string {
	return placeholderRe.ReplaceAllStringFunc(content, func(match string) string {
		key := strings.TrimSpace(placeholderRe.FindStringSubmatch(match)[1])
		if val, ok := variables[key]; ok {
			return val
		}
		return match
	})
}
