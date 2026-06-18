package llm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newOpenAITestServer(t *testing.T, status int, respBody string, check func(reqBody []byte, r *http.Request)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk-test" {
			t.Errorf("auth header = %q, want Bearer sk-test", got)
		}
		body, _ := io.ReadAll(r.Body)
		if check != nil {
			check(body, r)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(respBody))
	}))
}

func TestOpenAIClient_Success(t *testing.T) {
	srv := newOpenAITestServer(t, 200, `{
		"id": "chatcmpl-1",
		"object": "chat.completion",
		"created": 1,
		"model": "gpt-4o",
		"choices": [{"index":0,"message":{"role":"assistant","content":"{\"planId\":\"x\"}"},"finish_reason":"stop"}],
		"usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
	}`, func(body []byte, _ *http.Request) {
		var req map[string]any
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("unmarshal request: %v", err)
		}
		if req["model"] != "gpt-4o" {
			t.Errorf("model = %v, want gpt-4o", req["model"])
		}
		msgs, _ := req["messages"].([]any)
		if len(msgs) != 2 {
			t.Errorf("expected 2 messages (system+user), got %d", len(msgs))
		}
		first, _ := msgs[0].(map[string]any)
		if first["role"] != "system" {
			t.Errorf("first message role = %v, want system", first["role"])
		}
		rf, _ := req["response_format"].(map[string]any)
		if rf["type"] != "json_schema" {
			t.Errorf("response_format type = %v, want json_schema", rf["type"])
		}
	})
	defer srv.Close()

	c, err := NewOpenAIClient(Config{
		Provider: ProviderOpenAI,
		BaseURL:  srv.URL,
		APIKey:   "sk-test",
		Model:    "gpt-4o",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	resp, err := c.Complete(context.Background(), Request{
		System:         "you are a planner",
		Messages:       []Message{{Role: RoleUser, Content: []ContentPart{{Type: "text", Text: "plan"}}}},
		ResponseFormat: ResponseFormat{Type: ResponseFormatJSONSchema, JSONSchema: map[string]any{"type": "object", "title": "Plan"}},
		MaxTokens:      100,
		Temperature:    0,
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if resp.Text != `{"planId":"x"}` {
		t.Errorf("text = %q", resp.Text)
	}
	if resp.StopReason != "stop" {
		t.Errorf("stopReason = %q", resp.StopReason)
	}
	if resp.Usage.InputTokens != 10 || resp.Usage.OutputTokens != 5 {
		t.Errorf("usage = %+v", resp.Usage)
	}
}

func TestOpenAIClient_HTTPError(t *testing.T) {
	srv := newOpenAITestServer(t, 401, `{"error":"unauthorized"}`, nil)
	defer srv.Close()

	c, _ := NewOpenAIClient(Config{Provider: ProviderOpenAI, BaseURL: srv.URL, APIKey: "sk-test", Model: "gpt-4o"})
	_, err := c.Complete(context.Background(), Request{Messages: []Message{{Role: RoleUser, Content: []ContentPart{{Type: "text", Text: "x"}}}}})
	if err == nil {
		t.Fatal("expected error for 401")
	}
}

func TestOpenAIClient_NoChoices(t *testing.T) {
	srv := newOpenAITestServer(t, 200, `{"id":"x","object":"chat.completion","created":1,"model":"gpt-4o","choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}`, nil)
	defer srv.Close()

	c, _ := NewOpenAIClient(Config{Provider: ProviderOpenAI, BaseURL: srv.URL, APIKey: "sk-test", Model: "gpt-4o"})
	_, err := c.Complete(context.Background(), Request{Messages: []Message{{Role: RoleUser, Content: []ContentPart{{Type: "text", Text: "x"}}}}})
	if err == nil {
		t.Fatal("expected error for no choices")
	}
}

func TestOpenAIClient_UsesConfigModelWhenRequestEmpty(t *testing.T) {
	srv := newOpenAITestServer(t, 200, `{"id":"x","object":"chat.completion","created":1,"model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"{}"},"finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}`, func(body []byte, _ *http.Request) {
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		if req["model"] != "gpt-4o" {
			t.Errorf("model = %v, want gpt-4o from config", req["model"])
		}
	})
	defer srv.Close()

	c, _ := NewOpenAIClient(Config{Provider: ProviderOpenAI, BaseURL: srv.URL, APIKey: "sk-test", Model: "gpt-4o"})
	_, _ = c.Complete(context.Background(), Request{Messages: []Message{{Role: RoleUser, Content: []ContentPart{{Type: "text", Text: "x"}}}}})
}

func TestOpenAIClient_InvalidConfig(t *testing.T) {
	_, err := NewOpenAIClient(Config{Provider: ProviderOpenAI, BaseURL: "", APIKey: "", Model: ""})
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
}

func TestOpenAIClient_ResponseFormatText(t *testing.T) {
	srv := newOpenAITestServer(t, 200, `{"id":"x","object":"chat.completion","created":1,"model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}`, func(body []byte, _ *http.Request) {
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		rf, ok := req["response_format"].(map[string]any)
		if !ok {
			t.Fatal("expected response_format present")
		}
		if rf["type"] != "text" {
			t.Errorf("response_format type = %v, want text", rf["type"])
		}
	})
	defer srv.Close()

	c, _ := NewOpenAIClient(Config{Provider: ProviderOpenAI, BaseURL: srv.URL, APIKey: "sk-test", Model: "gpt-4o"})
	resp, err := c.Complete(context.Background(), Request{
		Messages:       []Message{{Role: RoleUser, Content: []ContentPart{{Type: "text", Text: "x"}}}},
		ResponseFormat: ResponseFormat{Type: ResponseFormatText},
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if resp.Text != "hello" {
		t.Errorf("text = %q", resp.Text)
	}
}

func TestSanitizeSchemaName(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"Plan", "Plan"},
		{"my plan!", "my_plan_"},
		{"", "plan"},
		{"a b c", "a_b_c"},
	}
	for _, c := range cases {
		if got := sanitizeSchemaName(c.in); got != c.want {
			t.Errorf("sanitizeSchemaName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
