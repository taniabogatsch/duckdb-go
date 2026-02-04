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

func TestScalarUDFChunk_Rows_Basic(t *testing.T) {
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

	// Test iteration and Args
	count := 0
	for row, idx := range chunk.Rows() {
		require.Equal(t, count, idx)
		require.Equal(t, count, row.Index())
		require.Len(t, row.Args, 2)
		require.Equal(t, input.rows[idx][0], row.Args[0])
		require.Equal(t, input.rows[idx][1], row.Args[1])
		count++
	}
	require.Equal(t, 3, count)
}

func TestScalarUDFChunk_Rows_EarlyBreak(t *testing.T) {
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
	for range chunk.Rows() {
		count++
		if count == 2 {
			break
		}
	}
	require.Equal(t, 2, count)
}

func TestScalarUDFChunk_Rows_NullInNullOut_SkipsNullRows(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1), int32(10)}, // no nulls - should be yielded
			{nil, int32(20)},      // has null - should be skipped
			{int32(3), nil},       // has null - should be skipped
			{int32(4), int32(40)}, // no nulls - should be yielded
		},
	}
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: true,
	}

	// Only rows 0 and 3 should be yielded
	yieldedIndices := []int{}
	for row, idx := range chunk.Rows() {
		yieldedIndices = append(yieldedIndices, idx)
		// Verify Args don't contain nil
		for _, arg := range row.Args {
			require.NotNil(t, arg)
		}
	}

	require.Equal(t, []int{0, 3}, yieldedIndices)

	// Verify NULL was set for skipped rows
	require.Nil(t, output.results[1])
	require.Nil(t, output.results[2])
}

func TestScalarUDFChunk_Rows_NullInNullOut_Disabled(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1), int32(10)},
			{nil, int32(20)}, // has null but should still be yielded
			{int32(3), nil},  // has null but should still be yielded
			{int32(4), int32(40)},
		},
	}
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: false, // disabled
	}

	// All rows should be yielded
	count := 0
	for range chunk.Rows() {
		count++
	}

	require.Equal(t, 4, count)
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

	for row := range chunk.Rows() {
		err := row.SetResult(42)
		require.ErrorIs(t, err, expectedErr)
	}
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
	for range chunk.Rows() {
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
			{nil, int32(30)}, // null in first column - should be skipped
			{int32(4), int32(40)},
		},
	}
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: true,
	}

	// Execute UDF - only non-null rows are yielded
	for row := range chunk.Rows() {
		result := row.Args[0].(int32) + row.Args[1].(int32)
		err := row.SetResult(result)
		require.NoError(t, err)
	}

	// Verify results
	require.Equal(t, int32(11), output.results[0])
	require.Equal(t, int32(22), output.results[1])
	require.Nil(t, output.results[2]) // null-in-null-out
	require.Equal(t, int32(44), output.results[3])
}

func TestScalarUDFChunk_Args_TypePreservation(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{int32(1), "hello", 3.14, true},
		},
	}

	chunk := &ScalarUDFChunk{
		input:  input,
		output: newMockChunkWriter(),
	}

	for row := range chunk.Rows() {
		require.Equal(t, int32(1), row.Args[0])
		require.Equal(t, "hello", row.Args[1])
		require.Equal(t, 3.14, row.Args[2])
		require.Equal(t, true, row.Args[3])
	}
}

func TestScalarUDFChunk_NullInNullOut_AllNullRow(t *testing.T) {
	input := &mockChunkReader{
		rows: [][]any{
			{nil, nil}, // all nulls
		},
	}
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: true,
	}

	count := 0
	for range chunk.Rows() {
		count++
	}

	require.Equal(t, 0, count)        // Row should be skipped
	require.Nil(t, output.results[0]) // Result should be NULL
}
