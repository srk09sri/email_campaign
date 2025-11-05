package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type EmailProvider interface {
	Send(email string) error
}

type MockProvider struct{}

func NewMockProvider() *MockProvider { return &MockProvider{} }
func (m *MockProvider) Send(email string) error {
	if strings.TrimSpace(email) == "" { return errors.New("empty email") }
	// simulate success quickly
	return nil
}

// Minimal SendGrid example (replace payload as you wish)
type SendGridProvider struct {
	apiKey string
	client *http.Client
}

func NewSendGridProvider(apiKey string) *SendGridProvider {
	return &SendGridProvider{
		apiKey: apiKey,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *SendGridProvider) Send(email string) error {
	if strings.TrimSpace(email) == "" { return errors.New("empty email") }

	payload := map[string]any{
		"personalizations": []map[string]any{{"to": []map[string]string{{"email": email}}}},
		"from":    map[string]string{"email": "no-reply@example.com"},
		"subject": "Campaign test email",
		"content": []map[string]string{{"type": "text/plain", "value": "Hello from campaign!"}},
	}
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", "https://api.sendgrid.com/v3/mail/send", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+s.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("sendgrid status=%d", resp.StatusCode)
}
