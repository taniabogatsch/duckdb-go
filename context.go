package duckdb

import (
	"context"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/mapping"
)

// contextStore stores the thread-safe context of a connection.
type contextStore struct {
	m sync.Map
}

// newContextStore creates a new instance of ctxStore.
func newContextStore() *contextStore {
	return &contextStore{
		m: sync.Map{},
	}
}

func (s *contextStore) load(connId uint64) context.Context {
	v, ok := s.m.Load(connId)
	if !ok {
		return context.Background()
	}
	ctx, ok := v.(context.Context)
	if !ok {
		return context.Background()
	}

	return ctx
}

func (s *contextStore) store(connId uint64, ctx context.Context, replace bool) func() {
	if !replace {
		_, ok := s.m.Load(connId)
		if ok {
			return func() {}
		}
	}

	s.m.Store(connId, ctx)

	return func() {
		s.delete(connId)
	}
}

func (s *contextStore) delete(connId uint64) {
	s.m.Delete(connId)
}

// runWithCtxInterrupt runs the function fn (which runs DuckDB mapping call/s),
// and robustly propagates ctx cancellation to DuckDB by repeatedly calling
// mapping.Interrupt on the given connection while the call is in flight.
//
// Semantics:
//   - Short-circuit - if ctx is already canceled, the call is not started and ctx.Err() is returned.
//   - While fn is executing, and after ctx is canceled, we repeatedly invoke
//     duckdb_interrupt(conn) until fn returns, for cases when the interruptions are cleared internally in DuckDB.
//   - The interrupt loop is strictly scoped to the lifetime of this call and
//     stops immediately when fn returns, to avoid goroutine leaks.
//   - We never call interrupt unless ctx is canceled.
func runWithCtxInterrupt(ctx context.Context, conn mapping.Connection, fn func() error) error {
	// Short circuit: do not start the DuckDB call if context is already canceled.
	if err := ctx.Err(); err != nil {
		return err
	}

	done := make(chan struct{})

	// Interrupter goroutine
	go func() {
		select {
		case <-ctx.Done():
		case <-done:
			// finished before cancellation
			return
		}

		// Re-assert interruption until the wrapped function finishes.
		for {
			select {
			case <-done:
				return
			default:
				mapping.Interrupt(conn)
				time.Sleep(200 * time.Microsecond)
			}
		}
	}()

	err := fn()

	close(done)
	return err
}
