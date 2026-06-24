package rag

import (
	"context"
	"fmt"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

type OpenAIEmbedder struct {
	client openai.Client
	model  string
}

func NewOpenAIEmbedder(baseURL, apiKey, model string) (*OpenAIEmbedder, error) {
	if baseURL == "" || apiKey == "" || model == "" {
		return nil, fmt.Errorf("embedder: baseURL, apiKey, and model are all required")
	}
	client := openai.NewClient(
		option.WithAPIKey(apiKey),
		option.WithBaseURL(baseURL),
	)
	return &OpenAIEmbedder{client: client, model: model}, nil
}

func (e *OpenAIEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	resp, err := e.client.Embeddings.New(ctx, openai.EmbeddingNewParams{
		Input: openai.EmbeddingNewParamsInputUnion{OfString: openai.String(text)},
		Model: openai.EmbeddingModel(e.model),
	})
	if err != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}
	if len(resp.Data) == 0 {
		return nil, fmt.Errorf("embed: empty response")
	}
	embedding := resp.Data[0].Embedding
	vec := make([]float32, len(embedding))
	for i, v := range embedding {
		vec[i] = float32(v)
	}
	return vec, nil
}
