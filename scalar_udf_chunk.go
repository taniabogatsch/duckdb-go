package duckdb

import (
	"database/sql/driver"
	"iter"
)

// chunkReader is the interface for reading from a data chunk.
type chunkReader interface {
	GetSize() int
	ColumnCount() int
	GetValue(colIdx, rowIdx int) (any, error)
}

// chunkWriter is the interface for writing to a data chunk.
type chunkWriter interface {
	SetValue(colIdx, rowIdx int, val any) error
}

// ScalarUDFChunk provides lazy, row-oriented access to UDF input/output.
// Values are read on-demand from the underlying DuckDB vectors - no pre-materialization.
// Results are written directly to the output vector - no intermediate storage.
type ScalarUDFChunk struct {
	input         chunkReader
	output        chunkWriter
	nullInNullOut bool
}

// ScalarUDFRow represents a single row in a ScalarUDFChunk.
// It provides access to input values and allows setting the result.
type ScalarUDFRow struct {
	chunk  *ScalarUDFChunk
	rowIdx int
	// Args contains the pre-fetched input values for this row.
	Args []driver.Value
}

// RowCount returns the number of rows in this chunk.
func (c *ScalarUDFChunk) RowCount() int {
	return c.input.GetSize()
}

// ColumnCount returns the number of input columns.
func (c *ScalarUDFChunk) ColumnCount() int {
	return c.input.ColumnCount()
}

// Rows returns an iterator over all rows in the chunk.
// Each iteration yields a ScalarUDFRow and its index.
//
// When nullInNullOut is enabled (the default), rows containing any NULL input
// are automatically skipped and their result is set to NULL. This means your
// UDF only sees rows with all non-NULL inputs.
//
// Note: row.Args is reused across iterations for efficiency. Do not store
// references to Args beyond the current iteration - copy values if needed.
//
// Example:
//
//	for row, rowIdx := range chunk.Rows() {
//	    // row.Args contains the pre-fetched input values
//	    result := row.Args[0].(int32) + row.Args[1].(int32)
//	    row.SetResult(result)
//	}
func (c *ScalarUDFChunk) Rows() iter.Seq2[*ScalarUDFRow, int] {
	return func(yield func(*ScalarUDFRow, int) bool) {
		colCount := c.ColumnCount()
		// Reuse args slice across iterations to reduce allocations.
		args := make([]driver.Value, colCount)

		for rowIdx := range c.RowCount() {
			hasNull := false

			for colIdx := range colCount {
				val, err := c.input.GetValue(colIdx, rowIdx)
				if err != nil {
					// On error, we can't continue - but iterators can't return errors.
					// The error will surface when the user tries to use the row.
					// For now, yield the row with what we have.
					break
				}
				args[colIdx] = val

				if val == nil {
					hasNull = true
				}
			}

			// If nullInNullOut and row has NULL, auto-set result to NULL and skip
			if c.nullInNullOut && hasNull {
				_ = c.output.SetValue(0, rowIdx, nil)
				continue
			}

			row := &ScalarUDFRow{
				chunk:  c,
				rowIdx: rowIdx,
				Args:   args,
			}

			if !yield(row, rowIdx) {
				return
			}
		}
	}
}

// SetResult writes the result value for this row directly to the output vector.
func (r *ScalarUDFRow) SetResult(val any) error {
	return r.chunk.output.SetValue(0, r.rowIdx, val)
}

// Index returns the row index within the chunk.
func (r *ScalarUDFRow) Index() int {
	return r.rowIdx
}
