package duckdb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/mapping"
	"github.com/stretchr/testify/require"
)

// Test that if the context is already canceled before entering the wrapper,
// the function is not executed and no interrupt is issued.
func TestRunWithCtxInterrupt_PreCanceled(t *testing.T) {
	// Stub Interrupt to count invocations.
	var count atomic.Int64
	orig := mapping.Interrupt
	mapping.Interrupt = func(_ mapping.Connection) {
		count.Add(1)
	}
	t.Cleanup(func() { mapping.Interrupt = orig })

	// Pre-cancel the context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	var dummyConn mapping.Connection
	err := runWithCtxInterrupt(ctx, dummyConn, func() error {
		called = true
		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, called, "fn should not be called when ctx is pre-canceled")
	require.Equal(t, int64(0), count.Load(), "Interrupt should not be called when ctx is pre-canceled")
}

// Test that when there is no cancellation, the wrapper does not call Interrupt.
func TestRunWithCtxInterrupt_NoCancel_NoInterrupt(t *testing.T) {
	var count atomic.Int64
	orig := mapping.Interrupt
	mapping.Interrupt = func(_ mapping.Connection) { count.Add(1) }
	t.Cleanup(func() { mapping.Interrupt = orig })

	ctx := context.Background()
	called := false
	var dummyConn mapping.Connection
	err := runWithCtxInterrupt(ctx, dummyConn, func() error {
		called = true
		return nil
	})

	require.NoError(t, err)
	require.True(t, called, "fn should be called when ctx is not canceled")
	require.Equal(t, int64(0), count.Load(), "Interrupt should not be called without cancellation")
}

// Test that after cancellation, Interrupt is invoked repeatedly until fn returns.
func TestRunWithCtxInterrupt_Cancel_RepeatsUntilDone(t *testing.T) {
	var count atomic.Int64
	orig := mapping.Interrupt
	mapping.Interrupt = func(_ mapping.Connection) { count.Add(1) }
	t.Cleanup(func() { mapping.Interrupt = orig })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	allowReturn := make(chan struct{})

	var dummyConn mapping.Connection
	doneErrCh := make(chan error)
	go func() {
		err := runWithCtxInterrupt(ctx, dummyConn, func() error {
			// Signal that fn is running.
			close(started)
			// Wait until test allows return, in the meantime, Interrupts should be happening.
			<-allowReturn
			return nil
		})
		doneErrCh <- err
	}()

	// Wait until the function has started, then cancel the context to trigger interrupts.
	<-started
	cancel()

	testTookTooLong := false
	// Wait until at least a few interrupts have been observed.
	deadline := time.Now().Add(100 * time.Millisecond)
	for count.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Microsecond)
		// break if the test takes too long before we have enough interrupts
		if time.Now().After(deadline.Add(1 * time.Second)) {
			testTookTooLong = true
			break
		}
	}
	require.GreaterOrEqual(t, count.Load(), int64(1), "Interrupt should be called after cancellation")

	// Allow the function to finish; wrapper should stop interrupting immediately after.
	close(allowReturn)
	err := <-doneErrCh
	require.NoError(t, err)
	require.False(t, testTookTooLong, "The test took too long without calling enough interruptions")
}
