package rag

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
)

type VectorDB interface {
	Insert(ctx context.Context, id string, vector []float32, metadata map[string]string) error
	Search(ctx context.Context, query []float32, topK int) ([]SearchResult, error)
}

type SearchResult struct {
	ID       string
	Score    float32
	Metadata map[string]string
}

type MemoryVectorDB struct {
	mu      sync.RWMutex
	dims    int
	entries []vectorEntry
}

type vectorEntry struct {
	id       string
	vector   []float32
	norm     float32
	metadata map[string]string
}

func NewMemoryVectorDB(dims int) *MemoryVectorDB {
	return &MemoryVectorDB{dims: dims}
}

func (db *MemoryVectorDB) Insert(_ context.Context, id string, vector []float32, metadata map[string]string) error {
	if len(vector) != db.dims {
		return fmt.Errorf("vector dimension mismatch: got %d, want %d", len(vector), db.dims)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.entries = append(db.entries, vectorEntry{
		id:       id,
		vector:   vector,
		norm:     l2Norm(vector),
		metadata: metadata,
	})
	return nil
}

func (db *MemoryVectorDB) Search(_ context.Context, query []float32, topK int) ([]SearchResult, error) {
	if len(query) != db.dims {
		return nil, fmt.Errorf("query dimension mismatch: got %d, want %d", len(query), db.dims)
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	queryNorm := l2Norm(query)
	results := make([]SearchResult, 0, len(db.entries))
	for _, e := range db.entries {
		score := cosineSimilarity(query, queryNorm, e.vector, e.norm)
		results = append(results, SearchResult{
			ID:       e.id,
			Score:    score,
			Metadata: e.metadata,
		})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	if topK > 0 && topK < len(results) {
		results = results[:topK]
	}
	return results, nil
}

func l2Norm(v []float32) float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	return float32(math.Sqrt(sum))
}

func cosineSimilarity(a []float32, aNorm float32, b []float32, bNorm float32) float32 {
	if aNorm == 0 || bNorm == 0 {
		return 0
	}
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return float32(dot / (float64(aNorm) * float64(bNorm)))
}
