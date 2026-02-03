package duckdb

import "database/sql/driver"

// ScalarFuncChunk provides lazy, row-oriented access to UDF input/output.
// Values are read on-demand from the underlying DuckDB vectors - no pre-materialization.
// Results are written directly to the output vector - no intermediate storage.
type ScalarFuncChunk struct {
	input         *DataChunk
	output        *DataChunk
	nullInNullOut bool
	nullRows      []bool // lazily populated when nullInNullOut is true
}

// RowCount returns the number of rows in this chunk.
func (c *ScalarFuncChunk) RowCount() int {
	return c.input.GetSize()
}

// ColumnCount returns the number of input columns.
func (c *ScalarFuncChunk) ColumnCount() int {
	return len(c.input.columns)
}

// GetValue reads a single value lazily from the input chunk.
// This goes directly to DuckDB's vector memory - no intermediate copy.
func (c *ScalarFuncChunk) GetValue(rowIdx, colIdx int) (driver.Value, error) {
	return c.input.GetValue(colIdx, rowIdx)
}

// SetResult writes a result value directly to the output vector.
// If nullInNullOut is enabled and any input in the row was NULL,
// the result is automatically set to NULL regardless of val.
func (c *ScalarFuncChunk) SetResult(rowIdx int, val any) error {
	if c.nullInNullOut && c.nullRows != nil && c.nullRows[rowIdx] {
		return c.output.SetValue(0, rowIdx, nil)
	}
	return c.output.SetValue(0, rowIdx, val)
}

// IsRowNull returns true if any input column in this row is NULL.
// Only meaningful when SpecialNullHandling is false (default).
// This is lazily computed on first access.
func (c *ScalarFuncChunk) IsRowNull(rowIdx int) bool {
	if !c.nullInNullOut {
		return false
	}
	if c.nullRows == nil {
		return false // Not tracked
	}
	return c.nullRows[rowIdx]
}

// checkRowNulls pre-scans a row for NULL values (for null-in-null-out handling).
// Called internally before user execution when SpecialNullHandling is false.
func (c *ScalarFuncChunk) checkRowNulls(rowIdx int) error {
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
