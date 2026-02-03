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
	nullRows      []bool // lazily populated when nullInNullOut is true
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
// Example:
//
//	for row, rowIdx := range chunk.Rows() {
//	    values := row.Values()
//	    // process values...
//	    row.SetResult(result)
//	}
func (c *ScalarUDFChunk) Rows() iter.Seq2[*ScalarUDFRow, int] {
	return func(yield func(*ScalarUDFRow, int) bool) {
		for i := range c.RowCount() {
			row := &ScalarUDFRow{chunk: c, rowIdx: i}
			if !yield(row, i) {
				return
			}
		}
	}
}

// ScalarUDFRow represents a single row in a ScalarUDFChunk.
// It provides access to input values and allows setting the result.
type ScalarUDFRow struct {
	chunk  *ScalarUDFChunk
	rowIdx int
}


// Values returns all input column values for this row.
// Values are read lazily from DuckDB's vector memory.
func (r *ScalarUDFRow) Values() ([]driver.Value, error) {
	colCount := r.chunk.ColumnCount()
	values := make([]driver.Value, colCount)
	for colIdx := range colCount {
		val, err := r.chunk.input.GetValue(colIdx, r.rowIdx)
		if err != nil {
			return nil, err
		}
		values[colIdx] = val
	}
	return values, nil
}

// Value returns a single input column value for this row.
func (r *ScalarUDFRow) Value(colIdx int) (driver.Value, error) {
	return r.chunk.input.GetValue(colIdx, r.rowIdx)
}

// SetResult writes the result value for this row directly to the output vector.
// If nullInNullOut is enabled and any input in the row was NULL,
// the result is automatically set to NULL regardless of val.
func (r *ScalarUDFRow) SetResult(val any) error {
	if r.chunk.nullInNullOut && r.chunk.nullRows != nil && r.chunk.nullRows[r.rowIdx] {
		return r.chunk.output.SetValue(0, r.rowIdx, nil)
	}
	return r.chunk.output.SetValue(0, r.rowIdx, val)
}

// IsNull returns true if any input column in this row is NULL.
// Only meaningful when SpecialNullHandling is false (default).
func (r *ScalarUDFRow) IsNull() bool {
	if !r.chunk.nullInNullOut {
		return false
	}
	if r.chunk.nullRows == nil {
		return false
	}
	return r.chunk.nullRows[r.rowIdx]
}

// Index returns the row index within the chunk.
func (r *ScalarUDFRow) Index() int {
	return r.rowIdx
}

// checkRowNulls pre-scans a row for NULL values (for null-in-null-out handling).
// Called internally before user execution when SpecialNullHandling is false.
func (c *ScalarUDFChunk) checkRowNulls(rowIdx int) error {
	if !c.nullInNullOut {
		return nil
	}
	if c.nullRows == nil {
		c.nullRows = make([]bool, c.RowCount())
	}
	for colIdx := range c.ColumnCount() {
		val, err := c.input.GetValue(colIdx, rowIdx)
		if err != nil {
			return err
		}
		if val == nil {
			c.nullRows[rowIdx] = true
			return nil
		}
	}
	return nil
}
