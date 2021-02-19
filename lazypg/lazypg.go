package lazypg

import (
	"context"
	"database/sql"
	"errors"

	"github.com/ghostec/transactioner"
	"github.com/ghostec/transactioner/pg"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type key string

const txKey = key("tx")

type Client struct {
	Pool pg.Pool
}

func (c *Client) Transaction(ctx context.Context) (*transactioner.Transaction, context.Context, error) {
	tx, tctx := transactioner.TransactionContext(ctx)

	var ltx *lazyTx
	ltxI := tctx.Value(txKey)
	if val, ok := ltxI.(*lazyTx); ok {
		ltx = val
	}

	if ltx != nil {
		return tx, tctx, nil
	}

	ltx = &lazyTx{pool: c.Pool, tx: tx}

	tx.Set(txKey, ltx)

	tx.RegisterExecute(func() error {
		pgtx, err := c.Pool.Begin(tctx)
		if err != nil {
			return errors.New("failed to begin pg transaction")
		}

		defer func() {
			if p := recover(); p != nil {
				// ensure a rollback attempt and panic again
				_ = pgtx.Rollback(tctx)
				panic(p)
			}
		}()

		for _, op := range ltx.operations {
			if err := op(); err != nil {
				return err
			}
		}

		if err := pgtx.Commit(tctx); err != nil && err != sql.ErrTxDone {
			if err := pgtx.Rollback(tctx); err != nil {
				return err
			}
			return err
		}

		return nil
	})

	return tx, tctx, nil
}

func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	return c.querier(ctx).Exec(ctx, query, args...)
}

func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	return c.querier(ctx).Query(ctx, query, args...)
}

func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
	return c.querier(ctx).QueryRow(ctx, query, args...)
}

func (c *Client) querier(ctx context.Context) pg.Querier {
	tx := transactioner.GetTransaction(ctx)

	if tx == nil {
		return c.Pool
	}

	ltx, ok := tx.Get(txKey).(*lazyTx)
	if !ok {
		return c.Pool
	}

	return ltx
}

type lazyTx struct {
	pool       pg.Pool
	operations []func() error
	tx         *transactioner.Transaction
}

func (t *lazyTx) Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	t.operations = append(t.operations, func() error {
		defer t.tx.Done()
		defer println("EXEC")
		_, err := t.pool.Exec(ctx, sql, arguments...)
		return err
	})
	return commandTag, nil
}

func (t *lazyTx) Query(ctx context.Context, sql string, args ...interface{}) (rows pgx.Rows, err error) {
	t.operations = append(t.operations, func() error {
		defer t.tx.Done()
		_, err := t.pool.Query(ctx, sql, args...)
		return err
	})
	return rows, nil
}

func (t *lazyTx) QueryRow(ctx context.Context, sql string, args ...interface{}) (row pgx.Row) {
	t.operations = append(t.operations, func() error {
		defer t.tx.Done()
		row := t.pool.QueryRow(ctx, sql, args...)
		return row.Scan()
	})
	return row
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

func (p *pool) Begin(ctx context.Context) (pg.Tx, error) {
	return p.Pool.Begin(ctx)
}
