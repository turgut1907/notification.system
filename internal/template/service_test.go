package template

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/turgut1907/notification.system/internal/domain"
)

type fakeTemplateRepo struct {
	tmpl domain.Template
	err  error
}

func (f fakeTemplateRepo) GetTemplateByID(_ context.Context, _ uuid.UUID) (domain.Template, error) {
	return f.tmpl, f.err
}

func TestRenderContent_SubstitutesKnownLeavesUnknown(t *testing.T) {
	out := RenderContent("Hi {{name}}, code {{code}} and {{missing}}", map[string]string{
		"name": "Sam",
		"code": "123",
	})
	want := "Hi Sam, code 123 and {{missing}}"
	if out != want {
		t.Fatalf("got %q want %q", out, want)
	}
}

func TestMissingVariables(t *testing.T) {
	missing := MissingVariables([]string{"a", "b", "c"}, map[string]string{"a": "x", "b": " "})
	if len(missing) != 2 || missing[0] != "b" || missing[1] != "c" {
		t.Fatalf("unexpected missing set: %v", missing)
	}
}

func TestRender_MissingRequiredReturnsTypedError(t *testing.T) {
	svc := New(fakeTemplateRepo{tmpl: domain.Template{
		Channel:           domain.ChannelSMS,
		Content:           "code {{code}}",
		RequiredVariables: []string{"code"},
	}})

	_, _, err := svc.Render(context.Background(), uuid.New(), domain.ChannelSMS, map[string]string{})
	if !errors.Is(err, domain.ErrTemplateVariablesMissing) {
		t.Fatalf("expected ErrTemplateVariablesMissing, got %v", err)
	}
}

func TestRender_ChannelMismatch(t *testing.T) {
	svc := New(fakeTemplateRepo{tmpl: domain.Template{
		Channel: domain.ChannelSMS,
		Content: "hi",
	}})

	_, _, err := svc.Render(context.Background(), uuid.New(), domain.ChannelEmail, nil)
	var ve *domain.ValidationError
	if !errors.As(err, &ve) || ve.Field != "channel" {
		t.Fatalf("expected channel validation error, got %v", err)
	}
}

func TestRender_Success(t *testing.T) {
	svc := New(fakeTemplateRepo{tmpl: domain.Template{
		Channel:           domain.ChannelSMS,
		Content:           "code {{code}}",
		RequiredVariables: []string{"code"},
	}})

	_, content, err := svc.Render(context.Background(), uuid.New(), domain.ChannelSMS, map[string]string{"code": "999"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if content != "code 999" {
		t.Fatalf("got %q", content)
	}
}
