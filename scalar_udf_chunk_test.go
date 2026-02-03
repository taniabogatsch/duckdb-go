package duckdb

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockChunkReader is a mock implementation of chunkReader for testing.
type mockChunkReader struct {
	rows     [][]any
	errOnGet error
}

func (m *mockChunkReader) GetSize() int {
	return len(m.rows)
}

func (m *mockChunkReader) ColumnCount() int {
	if len(m.rows) == 0 {
		return 0
	}
	return len(m.rows[0])
}

func (m *mockChunkReader) GetValue(colIdx, rowIdx int) (any, error) {
	if m.errOnGet != nil {
		return nil, m.errOnGet
	}
	if rowIdx < 0 || rowIdx >= len(m.rows) {
		return nil, errors.New("row index out of bounds")
	}
	if colIdx < 0 || colIdx >= len(m.rows[rowIdx]) {
		return nil, errors.New("column index out of bounds")
	}
	return m.rows[rowIdx][colIdx], nil
}

// mockChunkWriter is a mock implementation of chunkWriter for testing.
type mockChunkWriter struct {
	results  map[int]any
	errOnSet error
}

func newMockChunkWriter() *mockChunkWriter {
	return &mockChunkWriter{results: make(map[int]any)}
}

func (m *mockChunkWriter) SetValue(colIdx, rowIdx int, val any) error {
	if m.errOnSet != nil {
		return m.errOnSet
	}
	m.results[rowIdx] = val
	return nil
}

func TestScalarUDFChunk_RowCount(t *testing.T) {
	tests := []struct {
		name     string
		rows     [][]any
		expected int
	}{
		{"empty", [][]any{}, 0},
		{"single row", [][]any{{1, 2}}, 1},
		{"multiple rows", [][]any{{1, 2}, {3, 4}, {5, 6}}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunk := &ScalarUDFChunk{
				input: &mockChunkReader{rows: tt.rows},
			}
			require.Equal(t, tt.expected, chunk.RowCount())
		})
	}
}

func TestScalarUDFChunk_ColumnCount(t *testing.T) {
	tests := []struct {
		name     string
		rows     [][]any
		expected int
	}{
		{"empty", [][]any{}, 0},
		{"single column", [][]any{{1}}, 1},
		{"two columns", [][]any{{1, 2}}, 2},
		{"three columns", [][]any{{1, 2, 3}}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunk := &ScalarUDFChunk{
				input: &mockChunkReader{rows: tt.rows},
			}
			require.Equal(t, tt.expected, chunk.ColumnCount())
		})
	}
}

func TestScalarUDFChunk_Rows(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1), int32(10)},
			{int32(2), int32(20)},
			{int32(3), int32(30)},
		},
	}
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:  input,
		output: output,
	}

	// Test iteration
	count := 0
	for row, idx := range chunk.Rows() {
		require.Equal(t, count, idx)
		require.Equal(t, count, row.Index())
		count++
	}
	require.Equal(t, 3, count)
}

func TestScalarUDFChunk_RowsEarlyBreak(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1)},
			{int32(2)},
			{int32(3)},
		},
	}

	chunk := &ScalarUDFChunk{
		input:  input,
		output: newMockChunkWriter(),
	}

	// Test early break
	count := 0
	for _, _ = range chunk.Rows() {
		count++
		if count == 2 {
			break
		}
	}
	require.Equal(t, 2, count)
}

func TestScalarUDFRow_Values(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1), "hello", 3.14},
			{int32(2), "world", 2.71},
		},
	}

	chunk := &ScalarUDFChunk{
		input:  input,
		output: newMockChunkWriter(),
	}

	for row, idx := range chunk.Rows() {
		values, err := row.Values()
		require.NoError(t, err)
		require.Len(t, values, len(input.rows[idx]))
		for i, v := range values {
			require.Equal(t, input.rows[idx][i], v)
		}
	}
}

func TestScalarUDFRow_ValuesError(t *testing.T) {
	expectedErr := errors.New("test error")
	input := &mockChunkReader{
		rows:     [][]any{{1, 2}},
		errOnGet: expectedErr,
	}

	chunk := &ScalarUDFChunk{
		input:  input,
		output: newMockChunkWriter(),
	}

	for row, _ := range chunk.Rows() {
		_, err := row.Values()
		require.ErrorIs(t, err, expectedErr)
	}
}

func TestScalarUDFRow_Value(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1), "hello", 3.14},
		},
	}

	chunk := &ScalarUDFChunk{
		input:  input,
		output: newMockChunkWriter(),
	}

	for row, _ := range chunk.Rows() {
		val0, err := row.Value(0)
		require.NoError(t, err)
		require.Equal(t, int32(1), val0)

		val1, err := row.Value(1)
		require.NoError(t, err)
		require.Equal(t, "hello", val1)

		val2, err := row.Value(2)
		require.NoError(t, err)
		require.Equal(t, 3.14, val2)
	}
}

func TestScalarUDFRow_SetResult(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{{1}, {2}, {3}},
	}
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:  input,
		output: output,
	}

	for row, idx := range chunk.Rows() {
		err := row.SetResult(idx * 10)
		require.NoError(t, err)
	}

	require.Equal(t, 0, output.results[0])
	require.Equal(t, 10, output.results[1])
	require.Equal(t, 20, output.results[2])
}

func TestScalarUDFRow_SetResultError(t *testing.T) {
	expectedErr := errors.New("write error")
	input := &mockChunkReader{
		rows: [][]any{{1}},
	}
	output := &mockChunkWriter{
		results:  make(map[int]any),
		errOnSet: expectedErr,
	}

	chunk := &ScalarUDFChunk{
		input:  input,
		output: output,
	}

	for row, _ := range chunk.Rows() {
		err := row.SetResult(42)
		require.ErrorIs(t, err, expectedErr)
	}
}

func TestScalarUDFRow_IsNull_NullInNullOutDisabled(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{{nil}},
	}

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        newMockChunkWriter(),
		nullInNullOut: false,
	}

	for row, _ := range chunk.Rows() {
		require.False(t, row.IsNull())
	}
}

func TestScalarUDFRow_IsNull_NullInNullOutEnabled(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1), int32(2)}, // no nulls
			{nil, int32(2)},      // first column null
			{int32(1), nil},      // second column null
			{nil, nil},           // both null
		},
	}

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        newMockChunkWriter(),
		nullInNullOut: true,
	}

	// Pre-scan for nulls
	for rowIdx := range chunk.RowCount() {
		err := chunk.checkRowNulls(rowIdx)
		require.NoError(t, err)
	}

	expectedNulls := []bool{false, true, true, true}
	idx := 0
	for row, _ := range chunk.Rows() {
		require.Equal(t, expectedNulls[idx], row.IsNull(), "row %d", idx)
		idx++
	}
}

func TestScalarUDFRow_SetResult_NullInNullOut(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1), int32(2)}, // no nulls
			{nil, int32(2)},      // has null
		},
	}
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: true,
	}

	// Pre-scan for nulls
	for rowIdx := range chunk.RowCount() {
		err := chunk.checkRowNulls(rowIdx)
		require.NoError(t, err)
	}

	for row, _ := range chunk.Rows() {
		// Try to set result to 42 for all rows
		err := row.SetResult(int32(42))
		require.NoError(t, err)
	}

	// Row 0 should have 42, row 1 should have nil (due to null-in-null-out)
	require.Equal(t, int32(42), output.results[0])
	require.Nil(t, output.results[1])
}

func TestScalarUDFChunk_checkRowNulls(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1), int32(2)},
			{nil, int32(2)},
			{int32(1), nil},
		},
	}

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        newMockChunkWriter(),
		nullInNullOut: true,
	}

	// Check all rows
	for rowIdx := range chunk.RowCount() {
		err := chunk.checkRowNulls(rowIdx)
		require.NoError(t, err)
	}

	require.NotNil(t, chunk.nullRows)
	require.Equal(t, false, chunk.nullRows[0])
	require.Equal(t, true, chunk.nullRows[1])
	require.Equal(t, true, chunk.nullRows[2])
}

func TestScalarUDFChunk_checkRowNulls_Disabled(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{{nil}},
	}

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        newMockChunkWriter(),
		nullInNullOut: false,
	}

	err := chunk.checkRowNulls(0)
	require.NoError(t, err)
	require.Nil(t, chunk.nullRows) // Should not be allocated
}

func TestScalarUDFChunk_checkRowNulls_Error(t *testing.T) {
	expectedErr := errors.New("read error")
	input := &mockChunkReader{
		rows:     [][]any{{1, 2}},
		errOnGet: expectedErr,
	}

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        newMockChunkWriter(),
		nullInNullOut: true,
	}

	err := chunk.checkRowNulls(0)
	require.ErrorIs(t, err, expectedErr)
}

func TestScalarUDFRow_Index(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{{1}, {2}, {3}, {4}, {5}},
	}

	chunk := &ScalarUDFChunk{
		input:  input,
		output: newMockChunkWriter(),
	}

	for row, idx := range chunk.Rows() {
		require.Equal(t, idx, row.Index())
	}
}

func TestScalarUDFChunk_EmptyChunk(t *testing.T) {
	input := &mockChunkReader{rows: [][]any{}}
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:  input,
		output: output,
	}

	require.Equal(t, 0, chunk.RowCount())
	require.Equal(t, 0, chunk.ColumnCount())

	// Iteration should work but yield nothing
	count := 0
	for _, _ = range chunk.Rows() {
		count++
	}
	require.Equal(t, 0, count)
}

func TestScalarUDFChunk_Integration(t *testing.T) {
	// Simulate a typical UDF execution pattern
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1), int32(10)},
			{int32(2), int32(20)},
			{nil, int32(30)}, // null in first column
			{int32(4), int32(40)},
		},
	}
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: true,
	}

	// Pre-scan for nulls (like executeChunk does)
	for rowIdx := range chunk.RowCount() {
		err := chunk.checkRowNulls(rowIdx)
		require.NoError(t, err)
	}

	// Execute UDF
	for row, _ := range chunk.Rows() {
		if row.IsNull() {
			err := row.SetResult(nil)
			require.NoError(t, err)
			continue
		}

		values, err := row.Values()
		require.NoError(t, err)

		result := values[0].(int32) + values[1].(int32)
		err = row.SetResult(result)
		require.NoError(t, err)
	}

	// Verify results
	require.Equal(t, int32(11), output.results[0])
	require.Equal(t, int32(22), output.results[1])
	require.Nil(t, output.results[2]) // null-in-null-out
	require.Equal(t, int32(44), output.results[3])
}
