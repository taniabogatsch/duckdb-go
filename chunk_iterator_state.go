package duckdb

import (
	"database/sql/driver"
	"iter"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// ChunkIteratorState TODO comment
type ChunkIteratorState struct {
	r             Row
	output        *vector
	nullInNullOut bool
	Args          []driver.Value
}

// Rows TODO comment
func (iterState *ChunkIteratorState) Rows() (iter.Seq[*ChunkIteratorState], func() error) {
	var iterErr error
	seq := func(yield func(*ChunkIteratorState) bool) {
		colCount := iterState.r.chunk.ColumnCount()
		// Reuse args slice across iterations to reduce allocations.
		args := make([]driver.Value, colCount)

		for rowIdx := range iterState.r.chunk.GetSize() {
			hasNull := false

			for colIdx := range colCount {
				val, err := iterState.r.chunk.GetValue(colIdx, rowIdx)
				if err != nil {
					iterErr = err
					return
				}
				args[colIdx] = val

				if val == nil {
					hasNull = true
				}
			}

			// If nullInNullOut and row has NULL, auto-set result to NULL and skip
			if iterState.nullInNullOut && hasNull {
				iterErr = iterState.output.SetValue(rowIdx, nil)
				if iterErr != nil {
					// if we could not set a return value, stop iterating and surface the error
					return
				}
				continue
			}

			state := &ChunkIteratorState{
				r: Row{
					chunk: iterState.r.chunk,
					r:     mapping.IdxT(rowIdx),
				},
				output:        iterState.output,
				nullInNullOut: iterState.nullInNullOut,
				Args:          args,
			}

			wantNext := yield(state)
			if !wantNext {
				return
			}
		}
	}
	onFinish := func() error {
		return iterErr
	}
	return seq, onFinish
}

// SetResult TODO comment
func (iterState *ChunkIteratorState) SetResult(val any) error {
	return iterState.output.SetValue(int(iterState.r.r), val)
}
