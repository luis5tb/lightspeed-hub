package server

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Startup retry: survive transient Kubernetes API failures instead of
// crash-looping.
//
// LIVE INCIDENT (the motivation, recorded per the repo's hard-won-lessons
// style): the adapter exited fatally with
//
//	hub-adapter exiting: list agenticruns for recovery: storage is (re)initializing
//
// The AgenticRun CRDs are applied at Argo sync-wave -1 and the Deployment
// starts at wave 0, so the adapter's startup recovery LIST can race the API
// server's CRD storage establishment ("storage is (re)initializing"); the
// same error class also appears when etcd flaps. Both are TRANSIENT — the
// adapter must wait them out, not exit (a CrashLoopBackOff only widens the
// race window it is trying to survive).
//
// Non-transient errors (Forbidden, NotFound, invalid config) are still
// fatal: retrying an RBAC mistake for five minutes just hides the real
// problem from the operator.

// RetryOptions tunes RetryTransient. The zero value retries with the
// documented defaults; Timeout 0 means FAIL FAST (a single attempt, for
// tests and operators who want the old behavior).
type RetryOptions struct {
	// Timeout is the total wall-clock retry budget. 0 = fail fast.
	Timeout time.Duration
	// InitialBackoff is the first retry delay (default 2 s).
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential growth (default 30 s).
	MaxBackoff time.Duration
	// Logger receives one Warn line per retry (error, attempt, delay).
	Logger *slog.Logger
	// Now and Sleep are test hooks (virtual clock). Sleep must return
	// early with the context error when ctx is done.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
}

func (o *RetryOptions) fill() {
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 2 * time.Second
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 30 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Sleep == nil {
		o.Sleep = func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}
}

// RetryTransient runs fn until it succeeds, fn returns a NON-transient
// error (returned immediately — see IsTransientAPIError), or Timeout is
// exhausted (the last error is returned, unwrapped, so the exit log says
// the real thing). Each retry is logged with the attempt count and next
// delay. Exponential backoff: InitialBackoff, doubled per attempt, capped
// at MaxBackoff, with half-jitter (the actual delay is uniform in
// [delay/2, delay) — enough spread to de-synchronize pods without
// unbounded tails).
func RetryTransient(ctx context.Context, opts RetryOptions, fn func() error) error {
	opts.fill()
	deadline := opts.Now().Add(opts.Timeout)
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if opts.Timeout == 0 || !IsTransientAPIError(err) {
			return err
		}
		if !opts.Now().Before(deadline) {
			return err
		}
		delay := opts.InitialBackoff << (attempt - 1) // 2s, 4s, 8s, ...
		if delay > opts.MaxBackoff || delay <= 0 {    // <=0 guards shift overflow
			delay = opts.MaxBackoff
		}
		delay = delay/2 + time.Duration(rand.Int63n(int64(delay/2)))
		if wait := deadline.Sub(opts.Now()); delay > wait {
			delay = wait
		}
		opts.Logger.Warn("transient error during startup; retrying",
			"error", err, "attempt", attempt, "next_delay", delay.String(),
			"retry_timeout", opts.Timeout.String())
		if serr := opts.Sleep(ctx, delay); serr != nil {
			return serr
		}
	}
}

// RecoverWithRetry is Recover wrapped in RetryTransient: the startup
// recovery LIST is retried on transient API/storage errors (CRD storage
// still initializing after an Argo sync-wave race, flapping etcd) for up
// to opts.Timeout, and fails fast on everything else.
func (h *Handler) RecoverWithRetry(ctx context.Context, opts RetryOptions) error {
	return RetryTransient(ctx, opts, func() error { return h.Recover(ctx) })
}

// storageInitializingMsg is the API server's CRD-storage-establishment
// error text. It normally rides a 503 ServiceUnavailable, but the match
// is on the message so a reason change does not re-introduce the
// crash-loop the incident taught us about.
const storageInitializingMsg = "storage is (re)initializing"

// IsTransientAPIError reports whether err is a Kubernetes API failure
// that plausibly heals on its own: HTTP 503/504, server timeouts,
// deadline/connection-refused transport errors, and the CRD
// "storage is (re)initializing" race. Forbidden, NotFound, invalid
// config and everything else are non-transient.
func IsTransientAPIError(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsServiceUnavailable(err) || // 503: the storage-(re)initializing race, etcd flap
		apierrors.IsServerTimeout(err) || // 504 ServerTimeout
		apierrors.IsTimeout(err) || // 504 Timeout
		apierrors.IsInternalError(err) { // 500: etcd temporarily unavailable
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	// Message fallbacks: client-go wraps transport errors in *url.Error,
	// and the storage-initializing text has been observed with reasons
	// other than ServiceUnavailable. Walk the wrap chain (url.Error
	// implements Unwrap, so this descends into the transport cause too).
	for e := err; e != nil; e = errors.Unwrap(e) {
		if strings.Contains(e.Error(), storageInitializingMsg) ||
			strings.Contains(e.Error(), "connection refused") {
			return true
		}
	}
	return false
}
