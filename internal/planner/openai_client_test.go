package planner

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newOpenAITestServer(t *testing.T, status int, body string, check func([]byte)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk-test" {
			t.Errorf("auth header = %q", got)
		}
		reqBody, _ := io.ReadAll(r.Body)
		if check != nil {
			check(reqBody)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func TestOpenAIClient_Success(t *testing.T) {
	srv := newOpenAITestServer(t, 200, `{
		"id":"chatcmpl-1",
		"object":"chat.completion",
		"created":1,
		"model":"gpt-4o",
		"choices":[{"index":0,"message":{"role":"assistant","content":"{\"planId\":\"x\"}"},"finish_reason":"stop"}],
		"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}
	}`, func(body []byte) {
		req := parseOpenAIRequest(t, body)
		if req["model"] != "gpt-4o" {
			t.Errorf("model = %v", req["model"])
		}
		if req["max_tokens"] != float64(4096) {
			t.Errorf("max_tokens = %v", req["max_tokens"])
		}
		if req["reasoning_effort"] != "max" {
			t.Errorf("reasoning_effort = %v", req["reasoning_effort"])
		}
		messages := requestMessages(t, req)
		if len(messages) != 2 || messages[0]["role"] != "system" || messages[1]["role"] != "user" {
			t.Fatalf("messages = %#v", messages)
		}
		if responseFormat(t, req)["type"] != "json_object" {
			t.Errorf("response_format = %#v", req["response_format"])
		}
	})
	defer srv.Close()

	client, err := NewOpenAIClient(Config{
		BaseURL:         srv.URL,
		APIKey:          "sk-test",
		Model:           "gpt-4o",
		MaxTokens:       4096,
		ReasoningEffort: "max",
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	resp, err := client.Complete(context.Background(), LLMRequest{System: "system", Prompt: "plan"})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if resp.Text != `{"planId":"x"}` {
		t.Errorf("text = %q", resp.Text)
	}
}

func TestOpenAIClient_HTTPError(t *testing.T) {
	srv := newOpenAITestServer(t, 401, `{"error":"unauthorized"}`, nil)
	defer srv.Close()

	client, _ := NewOpenAIClient(Config{BaseURL: srv.URL, APIKey: "sk-test", Model: "gpt-4o"})
	if _, err := client.Complete(context.Background(), LLMRequest{Prompt: "x"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestOpenAIClient_NoChoices(t *testing.T) {
	srv := newOpenAITestServer(t, 200, `{"id":"x","object":"chat.completion","created":1,"model":"gpt-4o","choices":[]}`, nil)
	defer srv.Close()

	client, _ := NewOpenAIClient(Config{BaseURL: srv.URL, APIKey: "sk-test", Model: "gpt-4o"})
	if _, err := client.Complete(context.Background(), LLMRequest{Prompt: "x"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestOpenAIClient_InvalidConfig(t *testing.T) {
	if _, err := NewOpenAIClient(Config{}); err == nil {
		t.Fatal("expected error")
	}
}

func parseOpenAIRequest(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	return req
}

func requestMessages(t *testing.T, req map[string]any) []map[string]any {
	t.Helper()
	rawMessages, ok := req["messages"].([]any)
	if !ok {
		t.Fatalf("messages = %#v", req["messages"])
	}
	messages := make([]map[string]any, 0, len(rawMessages))
	for _, rawMessage := range rawMessages {
		message, ok := rawMessage.(map[string]any)
		if !ok {
			t.Fatalf("message = %#v", rawMessage)
		}
		messages = append(messages, message)
	}
	return messages
}

func responseFormat(t *testing.T, req map[string]any) map[string]any {
	t.Helper()
	format, ok := req["response_format"].(map[string]any)
	if !ok {
		t.Fatalf("response_format = %#v", req["response_format"])
	}
	return format
}
