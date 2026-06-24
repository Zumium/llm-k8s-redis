package rag

import (
	"context"
	"math"
	"sync"
	"testing"
)

func TestCosineSimilarity_Identical(t *testing.T) {
	v := []float32{1, 2, 3}
	norm := l2Norm(v)
	score := cosineSimilarity(v, norm, v, norm)
	if math.Abs(float64(score-1.0)) > 1e-6 {
		t.Fatalf("identical vectors should score 1.0, got %f", score)
	}
}

func TestCosineSimilarity_Orthogonal(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}
	score := cosineSimilarity(a, l2Norm(a), b, l2Norm(b))
	if math.Abs(float64(score)) > 1e-6 {
		t.Fatalf("orthogonal vectors should score 0, got %f", score)
	}
}

func TestCosineSimilarity_ZeroVector(t *testing.T) {
	a := []float32{0, 0, 0}
	b := []float32{1, 2, 3}
	score := cosineSimilarity(a, l2Norm(a), b, l2Norm(b))
	if score != 0 {
		t.Fatalf("zero vector should score 0, got %f", score)
	}
}

func TestMemoryVectorDB_InsertSearch(t *testing.T) {
	ctx := context.Background()
	db := NewMemoryVectorDB(3)

	if err := db.Insert(ctx, "a", []float32{1, 0, 0}, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := db.Insert(ctx, "b", []float32{0, 1, 0}, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}

	results, err := db.Search(ctx, []float32{1, 0, 0}, 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].ID != "a" {
		t.Fatalf("expected 'a' as top result, got %s", results[0].ID)
	}
	if results[0].Score <= results[1].Score {
		t.Fatalf("scores not sorted: %f <= %f", results[0].Score, results[1].Score)
	}
}

func TestMemoryVectorDB_TopK(t *testing.T) {
	ctx := context.Background()
	db := NewMemoryVectorDB(3)
	db.Insert(ctx, "a", []float32{1, 0, 0}, nil)
	db.Insert(ctx, "b", []float32{0, 1, 0}, nil)
	db.Insert(ctx, "c", []float32{0, 0, 1}, nil)

	results, _ := db.Search(ctx, []float32{1, 0, 0}, 1)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != "a" {
		t.Fatalf("expected 'a', got %s", results[0].ID)
	}
}

func TestMemoryVectorDB_DimensionMismatch(t *testing.T) {
	ctx := context.Background()
	db := NewMemoryVectorDB(3)
	if err := db.Insert(ctx, "x", []float32{1, 2}, nil); err == nil {
		t.Fatal("expected dimension mismatch on insert")
	}
	if _, err := db.Search(ctx, []float32{1, 2}, 1); err == nil {
		t.Fatal("expected dimension mismatch on search")
	}
}

func TestMemoryVectorDB_Concurrency(t *testing.T) {
	ctx := context.Background()
	db := NewMemoryVectorDB(3)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = db.Insert(ctx, "x", []float32{1, 0, 0}, nil)
			_, _ = db.Search(ctx, []float32{1, 0, 0}, 10)
		}(i)
	}
	wg.Wait()
}

func TestMemoryVectorDB_Metadata(t *testing.T) {
	ctx := context.Background()
	db := NewMemoryVectorDB(3)
	meta := map[string]string{"cluster": "example", "shards": "3"}
	db.Insert(ctx, "a", []float32{1, 0, 0}, meta)

	results, _ := db.Search(ctx, []float32{1, 0, 0}, 1)
	if results[0].Metadata["cluster"] != "example" {
		t.Fatalf("metadata mismatch: %v", results[0].Metadata)
	}
}
