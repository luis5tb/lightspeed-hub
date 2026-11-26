package server

import (
	"context"
	"fmt"

	agenticv1alpha1 "github.com/openshift/lightspeed-agentic-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	"github.com/openshift/lightspeed-hub/a2a-adapter/internal/run"
)

// latestAnalysisIdentity returns UID/generation for the newest successful
// analysis only after the Analyzed condition has observed the current run
// generation. A stale status ref during revision therefore cannot authorize
// execution credentials.
func (h *Handler) latestAnalysisIdentity(ctx context.Context, runCR *agenticv1alpha1.AgenticRun) (string, bool) {
	condition := findCondition(runCR, agenticv1alpha1.AgenticRunConditionAnalyzed)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration < runCR.Generation {
		return "", false
	}
	name, ok := newestSucceeded(runCR.Status.Steps.Analysis.Results)
	if !ok {
		return "", false
	}
	result, err := h.deps.Results.GetAnalysisResult(ctx, h.cfg.Namespace, name)
	if err != nil || result == nil || result.UID == "" || result.Spec.AgenticRunName != runCR.Name {
		return "", false
	}
	return fmt.Sprintf("%s/%d", result.UID, result.Generation), true
}

func executionStage(approval *agenticv1alpha1.AgenticRunApproval) *agenticv1alpha1.ApprovalStage {
	if approval == nil {
		return nil
	}
	for i := range approval.Spec.Stages {
		if approval.Spec.Stages[i].Type == agenticv1alpha1.ApprovalStageExecution {
			return &approval.Spec.Stages[i]
		}
	}
	return nil
}

func executionWasDecided(approval *agenticv1alpha1.AgenticRunApproval) bool {
	stage := executionStage(approval)
	// The operator treats a present stage with no explicit Decision as
	// approved. Mirror that API/CLI representation so revisions cannot bypass
	// an approval the operator already considers effective.
	return stage != nil
}

func effectiveApprovalDecision(decision agenticv1alpha1.ApprovalDecision) agenticv1alpha1.ApprovalDecision {
	if decision == "" {
		return agenticv1alpha1.ApprovalDecisionApproved
	}
	return decision
}

func executionStageApproved(approval *agenticv1alpha1.AgenticRunApproval) bool {
	stage := executionStage(approval)
	return stage != nil && effectiveApprovalDecision(stage.Decision) == agenticv1alpha1.ApprovalDecisionApproved
}

func sameOption(a, b *int32) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// executionCredentialAllowed is recomputed from durable state. Missing or
// unreadable approval/analysis data fails closed; the sweeper will retry a
// failed Secret removal on its next pass.
func (h *Handler) executionCredentialAllowed(ctx context.Context, runCR *agenticv1alpha1.AgenticRun) bool {
	if executionFinished(runCR) {
		return false
	}
	approval, err := h.deps.Results.GetApproval(ctx, h.cfg.Namespace, runCR.Name)
	if err != nil || !executionStageApproved(approval) {
		return false
	}
	identity, current := h.latestAnalysisIdentity(ctx, runCR)
	return current && identity != "" && runCR.Annotations[run.AnnotationApprovedAnalysis] == identity
}

// recordApprovedAnalysis writes the analysis identity and verified principal
// using the adapter-SA client. Caller approval updates remain caller-authenticated.
func (h *Handler) recordApprovedAnalysis(ctx context.Context, runCR *agenticv1alpha1.AgenticRun, principal, identity string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := h.deps.Readers.GetRun(ctx, h.cfg.Namespace, runCR.Name)
		if err != nil {
			return fmt.Errorf("get AgenticRun for approval attribution: %w", err)
		}
		if current.UID != runCR.UID || principal == "" || current.Annotations[run.AnnotationPrincipal] != principal {
			return fmt.Errorf("AgenticRun ownership changed before approval attribution")
		}
		if current.Annotations == nil {
			current.Annotations = map[string]string{}
		}
		current.Annotations[run.AnnotationApprovedAnalysis] = identity
		if err := h.deps.Readers.UpdateRun(ctx, current); err != nil {
			return fmt.Errorf("record approved analysis identity on AgenticRun: %w", err)
		}
		return nil
	})
}
