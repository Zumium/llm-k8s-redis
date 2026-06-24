package rag

import (
	"context"
	"fmt"
	"strconv"
)

type RetrievedPlan struct {
	Score       float32
	PlanSummary string
	ClusterName string
	Shards      int32
	Replicas    int32
}

type RequestSummary struct {
	ClusterName string
	Shards      int32
	Replicas    int32
	Image       string
	MemorySize  string
	NodeCount   int
	MasterCount int
}

type PlanSummary struct {
	PlanID    string
	Summary   string
	StepsText string
}

type PlanRetriever struct {
	db       VectorDB
	embedder Embedder
}

func NewPlanRetriever(db VectorDB, embedder Embedder) *PlanRetriever {
	return &PlanRetriever{db: db, embedder: embedder}
}

func (r *PlanRetriever) Retrieve(ctx context.Context, req RequestSummary, topK int) ([]RetrievedPlan, error) {
	queryText := requestSummaryText(req)
	vec, err := r.embedder.Embed(ctx, queryText)
	if err != nil {
		return nil, fmt.Errorf("retrieve embed: %w", err)
	}
	results, err := r.db.Search(ctx, vec, topK)
	if err != nil {
		return nil, fmt.Errorf("retrieve search: %w", err)
	}
	var plans []RetrievedPlan
	for _, result := range results {
		shards, _ := strconv.Atoi(result.Metadata["shards"])
		replicas, _ := strconv.Atoi(result.Metadata["replicas"])
		plans = append(plans, RetrievedPlan{
			Score:       result.Score,
			PlanSummary: result.Metadata["summary"],
			ClusterName: result.Metadata["clusterName"],
			Shards:      int32(shards),
			Replicas:    int32(replicas),
		})
	}
	return plans, nil
}

func (r *PlanRetriever) Store(ctx context.Context, req RequestSummary, p PlanSummary) error {
	text := requestSummaryText(req) + "\n" + planSummaryText(p)
	vec, err := r.embedder.Embed(ctx, text)
	if err != nil {
		return fmt.Errorf("store embed: %w", err)
	}
	meta := map[string]string{
		"clusterName": req.ClusterName,
		"shards":      strconv.Itoa(int(req.Shards)),
		"replicas":    strconv.Itoa(int(req.Replicas)),
		"summary":     p.StepsText,
	}
	return r.db.Insert(ctx, p.PlanID, vec, meta)
}

func requestSummaryText(req RequestSummary) string {
	return fmt.Sprintf("RedisCluster %s: %d shards, %d replicas. %s %s.\n%d nodes: %d masters.",
		req.ClusterName, req.Shards, req.Replicas,
		req.Image, req.MemorySize,
		req.NodeCount, req.MasterCount)
}

func planSummaryText(p PlanSummary) string {
	return fmt.Sprintf("Plan %s: %s. Steps: %s.", p.PlanID, p.Summary, p.StepsText)
}
