package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/duckdb/duckdb-go/mapping"
)

// Appender wraps functionality around the DuckDB appender.
// It enables efficient bulk transformations.
type Appender struct {
	// The raw sql.Conn's driver connection.
	conn *Conn
	// The DuckDB appender.
	appender mapping.Appender
	// True, if the appender has been closed.
	closed bool
	// True, if the appender has been invalidated.
	invalidated bool

	// The chunk to append to.
	chunk DataChunk
	// The column types of the table to append to.
	types []mapping.LogicalType
	// The number of appended rows.
	rowCount int
}

// NewAppenderFromConn returns a new Appender for the default catalog.
// The Appender batches rows via AppendRow. Upon reaching the auto-flush threshold or
// upon calling Flush or Close, it appends these rows to the table.
// Thus, it can be used instead of INSERT INTO statements to enable bulk insertions.
// `driverConn` is the raw sql.Conn's driver connection.
// `schema` and `table` specify the table (`schema.table`) to append to.
func NewAppenderFromConn(driverConn driver.Conn, schema, table string) (*Appender, error) {
	return NewAppender(driverConn, "", schema, table)
}

// NewAppender returns a new Appender.
// The Appender batches rows via AppendRow. Upon reaching the auto-flush threshold or
// upon calling Flush or Close, it appends these rows to the table.
// Thus, it can be used instead of INSERT INTO statements to enable bulk insertions.
// `driverConn` is the raw sql.Conn's driver connection.
// `catalog`, `schema` and `table` specify the table (`catalog.schema.table`) to append to.
func NewAppender(driverConn driver.Conn, catalog, schema, table string) (*Appender, error) {
	var a Appender
	err := a.appenderConn(driverConn)
	if err != nil {
		return nil, err
	}

	state := mapping.AppenderCreateExt(a.conn.conn, catalog, schema, table, &a.appender)
	if state == mapping.StateError {
		err = errorDataError(mapping.AppenderErrorData(a.appender))
		mapping.AppenderDestroy(&a.appender)
		return nil, getError(errAppenderCreation, err)
	}

	// Get the column types.
	columnCount := mapping.AppenderColumnCount(a.appender)
	for i := mapping.IdxT(0); i < columnCount; i++ {
		colType := mapping.AppenderColumnType(a.appender, i)
		a.types = append(a.types, colType)

		// Ensure that we only create an appender for supported column types.
		t := mapping.GetTypeId(colType)
		name, found := unsupportedTypeToStringMap[t]
		if found {
			err = addIndexToError(unsupportedTypeError(name), int(i)+1)
			destroyLogicalTypes(a.types)
			mapping.AppenderDestroy(&a.appender)
			return nil, getError(errAppenderCreation, err)
		}
	}

	return a.initAppenderChunk()
}

// NewQueryAppender returns a new query Appender.
// The Appender batches rows via AppendRow. Upon reaching the auto-flush threshold or
// upon calling Flush or Close, it executes the query, treating the batched rows as a temporary table.
// `driverConn` is the raw sql.Conn's driver connection.
// `query` is the query to execute. It can be a INSERT, DELETE, UPDATE or MERGE INTO statement.
// `table` is the (optional) table name of the temporary table containing the batched rows.
// It defaults to `appended_data`.
// `colTypes` are the column types of the temporary table.
// `colNames` are the (optional) names of the columns of the temporary table containing the batched rows.
// They default to `col1`, `col2`, ...
func NewQueryAppender(driverConn driver.Conn, query, table string, colTypes []TypeInfo, colNames []string) (*Appender, error) {
	var a Appender
	err := a.appenderConn(driverConn)
	if err != nil {
		return nil, err
	}

	if query == "" {
		return nil, getError(errAppenderEmptyQuery, nil)
	}
	if len(colTypes) == 0 {
		return nil, getError(errAppenderEmptyColumnTypes, nil)
	}
	if len(colNames) != 0 && len(colTypes) != 0 {
		if len(colNames) != len(colTypes) {
			return nil, getError(errAppenderColumnMismatch, nil)
		}
	}

	// Get the logical types via the type infos.
	for _, ct := range colTypes {
		a.types = append(a.types, ct.logicalType())
	}

	state := mapping.AppenderCreateQuery(a.conn.conn, query, a.types, table, colNames, &a.appender)
	if state == mapping.StateError {
		destroyLogicalTypes(a.types)
		err = errorDataError(mapping.AppenderErrorData(a.appender))
		mapping.AppenderDestroy(&a.appender)
		return nil, getError(errAppenderCreation, err)
	}

	return a.initAppenderChunk()
}

// Flush the data chunks to the underlying table and clear the internal cache.
// Does not close the appender, even if it returns an error. Unless you have a good reason to call this,
// call Close when you are done with the appender.
func (a *Appender) Flush() error {
	if a.invalidated {
		return getError(errAppenderInvalidated, nil)
	}
	if err := a.appendDataChunk(); err != nil {
		a.invalidated = true
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}
	if err := a.flush(); err != nil {
		a.invalidated = true
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}

	return nil
}

// FlushWithCancel flushes the data chunks to the underlying table and clears the internal cache.
// Does not close the appender, even if it returns an error. Unless you have a good reason to call this,
// call CloseWithCancel when you are done with the appender.
// Takes a context for cancellation.
func (a *Appender) FlushWithCancel(ctx context.Context) error {
	if a.invalidated {
		return getError(errAppenderInvalidated, nil)
	}
	if err := a.appendDataChunk(); err != nil {
		a.invalidated = true
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}

	if err := a.flushWithCancel(ctx); err != nil {
		a.invalidated = true
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}

	return nil
}

// Close the appender. This will flush the appender to the underlying table.
// It is vital to call this when you are done with the appender to avoid leaking memory.
// You also must call this when the appender has been invalidated.
func (a *Appender) Close() error {
	return a.close(nil, func(ctx *context.Context) error {
		return a.flush()
	})
}

// CloseWithCancel the appender. This will flush the appender to the underlying table.
// It is vital to call this when you are done with the appender to avoid leaking memory.
// Takes a context for cancellation.
func (a *Appender) CloseWithCancel(ctx context.Context) error {
	return a.close(&ctx, func(innerCtx *context.Context) error {
		return a.FlushWithCancel(*innerCtx)
	})
}

// AppendRow loads a row of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendRow(args ...driver.Value) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
	}

	err := a.appendRowSlice(args)
	if err != nil {
		return getError(errAppenderAppendRow, err)
	}

	return nil
}

func (a *Appender) appenderConn(driverConn driver.Conn) error {
	var ok bool
	a.conn, ok = driverConn.(*Conn)
	if !ok {
		return getError(errInvalidCon, nil)
	}
	if a.conn.closed {
		return getError(errClosedCon, nil)
	}

	return nil
}

func (a *Appender) initAppenderChunk() (*Appender, error) {
	if err := a.chunk.initFromTypes(a.types, true); err != nil {
		a.chunk.close()
		destroyLogicalTypes(a.types)
		mapping.AppenderDestroy(&a.appender)
		return nil, getError(errAppenderCreation, err)
	}

	return a, nil
}

func (a *Appender) appendRowSlice(args []driver.Value) error {
	// Early-out, if the number of args does not match the column count.
	if len(args) != len(a.types) {
		return columnCountError(len(args), len(a.types))
	}

	// Create a new data chunk if the current chunk is full.
	if a.rowCount == GetDataChunkCapacity() {
		if err := a.appendDataChunk(); err != nil {
			return err
		}
	}

	// Set all values.
	for i, val := range args {
		err := a.chunk.SetValue(i, a.rowCount, val)
		if err != nil {
			return err
		}
	}
	a.rowCount++

	return nil
}

func (a *Appender) appendDataChunk() error {
	if a.rowCount == 0 {
		// Nothing to append.
		return nil
	}
	if err := a.chunk.SetSize(a.rowCount); err != nil {
		return err
	}
	if mapping.AppendDataChunk(a.appender, a.chunk.chunk) == mapping.StateError {
		return getDuckDBError(mapping.AppenderError(a.appender))
	}

	a.chunk.reset(true)
	a.rowCount = 0

	return nil
}

func (a *Appender) flush() error {
	if mapping.AppenderFlush(a.appender) == mapping.StateError {
		return getDuckDBError(mapping.AppenderError(a.appender))
	}
	return nil
}

func (a *Appender) flushWithCancel(ctx context.Context) error {
	// Spawn go-routine waiting to receive on the context or main channel.
	mainDoneCh := make(chan struct{})
	bgDoneCh := make(chan struct{})
	go interruptRoutine(&mainDoneCh, &bgDoneCh, ctx, a.conn)

	err := a.flush()

	// We finished flushing the Appender.
	// Close the main channel.
	close(mainDoneCh)

	// Wait for the background go-routine to finish, too.
	// Sometimes the go-routine is not scheduled immediately.
	// By the time it is scheduled, another query might be running on this connection.
	// If we don't wait for the go-routine to finish, it can cancel that new query.
	<-bgDoneCh

	return err
}

func (a *Appender) close(ctx *context.Context, flushFn func(ctx *context.Context) error) error {
	if a.closed {
		return getError(errAppenderDoubleClose, nil)
	}
	a.closed = true

	// Append all remaining chunks.
	errAppend := a.appendDataChunk()
	a.chunk.close()

	// We flush before closing to get a meaningful error message.
	errFlush := flushFn(ctx)

	// Destroy all appender data and the appender.
	destroyLogicalTypes(a.types)
	var errClose error

	if ctx == nil {
		if mapping.AppenderDestroy(&a.appender) == mapping.StateError {
			errClose = errAppenderClose
		}
	} else {
		// Spawn go-routine waiting to receive on the context or main channel.
		mainDoneCh := make(chan struct{})
		bgDoneCh := make(chan struct{})
		go interruptRoutine(&mainDoneCh, &bgDoneCh, *ctx, a.conn)

		if mapping.AppenderDestroy(&a.appender) == mapping.StateError {
			errClose = errAppenderClose
		}

		// We finished flushing the Appender.
		// Close the main channel.
		close(mainDoneCh)

		// Wait for the background go-routine to finish, too.
		// Sometimes the go-routine is not scheduled immediately.
		// By the time it is scheduled, another query might be running on this connection.
		// If we don't wait for the go-routine to finish, it can cancel that new query.
		<-bgDoneCh
	}

	err := errors.Join(errAppend, errFlush, errClose)
	if err != nil {
		return getError(invalidatedAppenderError(err), nil)
	}

	return nil
}
