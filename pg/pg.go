package pg

import (
	"context"
	"database/sql"
	"errors"

	"github.com/ghostec/transactioner"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type ctxKey string

const txKey = ctxKey("tx")

// Querier should be used when either a transaction or a common connection could be used
type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

type Pool interface {
	Querier
	Begin(context.Context) (Tx, error)
}

type Tx interface {
	Querier
	Commit(context.Context) error
	Rollback(context.Context) error
}

type Client struct {
	Pool Pool
}

func (c *Client) Transaction(ctx context.Context) (*transactioner.Transaction, context.Context, error) {
	tx, tctx := transactioner.TransactionContext(ctx)

	var pgtx Tx
	pgtxI := tctx.Value(txKey)
	if val, ok := pgtxI.(pgx.Tx); ok {
		pgtx = val
	}

	if pgtx == nil {
		return tx, tctx, nil
	}

	var err error

	pgtx, err = c.Pool.Begin(tctx)
	if err != nil {
		return nil, nil, errors.New("failed to begin pg transaction")
	}

	txw := &txWrapper{Tx: pgtx}

	tx.Set(txKey, txw)

	tx.RegisterExecute(func() error {
		defer func() {
			if p := recover(); p != nil {
				// ensure a rollback attempt and panic again
				_ = txw.Rollback(tctx)
				panic(p)
			}
		}()

		if err := txw.Commit(tctx); err != nil && err != sql.ErrTxDone {
			if err := txw.Rollback(tctx); err != nil {
				return err
			}
			return err
		}

		return nil
	})
	return tx, tctx, nil
}

func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	defer transactioner.GetTransaction(ctx).Done()
	defer println("EXEC")
	return c.querier(ctx).Exec(ctx, query, args...)
}

func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	return c.querier(ctx).Query(ctx, query, args...)
}

func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	return c.querier(ctx).QueryRow(ctx, query, args...)
}

func (c *Client) querier(ctx context.Context) Querier {
	if tx, ok := ctx.Value(txKey).(*pgx.Tx); ok {
		return *tx
	}

	return c.Pool
}

func New(ctx context.Context, addr string) (*Client, error) {
	pgxPool, err := pgxpool.Connect(ctx, addr)
	if err != nil {
		return nil, err
	}

	return &Client{Pool: &pool{Pool: pgxPool}}, nil
}

type pool struct {
	*pgxpool.Pool
}

func (p *pool) Begin(ctx context.Context) (Tx, error) {
	return p.Pool.Begin(ctx)
}

type txWrapper struct {
	Tx
}

func (t *txWrapper) Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	defer transactioner.GetTransaction(ctx).Done()
	return t.Tx.Exec(ctx, query, args...)
}

func (t *txWrapper) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	defer transactioner.GetTransaction(ctx).Done()
	return t.Tx.Query(ctx, query, args...)
}

func (t *txWrapper) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	defer transactioner.GetTransaction(ctx).Done()
	return t.Tx.QueryRow(ctx, query, args...)
}
