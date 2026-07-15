package duckdb

import (
	"database/sql/driver"
	"iter"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// ChunkIteratorState is the chunk-based iterator passed to a ChunkContextExecutorFn.
// It iterates over its rows via Rows(). Copy the Args if you need to, as they are
// not retained between loop iterations.
type ChunkIteratorState struct {
	r             Row
	output        *vector
	nullInNullOut bool
	args          []driver.Value
}

// SetResult sets the current row's output value.
// Call once per yielded row.
func (iterState *ChunkIteratorState) SetResult(val any) error {
	return iterState.output.SetValue(int(iterState.r.rowIdx), val)
}

// GetValue TODO: comment
func (iterState *ChunkIteratorState) GetValue(colIdx int) driver.Value {
	return iterState.args[colIdx]
}

// Rows TODO: comment
func (iterState *ChunkIteratorState) Rows() iter.Seq2[*ChunkIteratorState, error] {
	colCount := iterState.r.chunk.ColumnCount()

	return func(yield func(*ChunkIteratorState, error) bool) {
		var err error
		for rowIdx := range iterState.r.chunk.GetSize() {
			hasNull := false
			for colIdx := range colCount {
				// FIXME: Could be replaced with a vectorized getter function.
				iterState.args[colIdx], err = iterState.r.chunk.GetValue(colIdx, rowIdx)
				if err != nil {
					yield(nil, err)
					return
				}
				if iterState.args[colIdx] == nil {
					hasNull = true
					break
				}
			}

			if iterState.nullInNullOut && hasNull {
				if err = iterState.output.SetValue(rowIdx, nil); err != nil {
					yield(nil, err)
					return
				}
				continue
			}

			iterState.r.rowIdx = mapping.IdxT(rowIdx)
			if !yield(iterState, nil) {
				return
			}
		}
	}
}
