package main

import (
	"context"

	"github.com/ghostec/transactioner"
	"github.com/ghostec/transactioner/lazypg"
	"github.com/ghostec/transactioner/pg"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

func main() {
	ctx := context.Background()
	lpg := &lazypg.Client{Pool: &mockedPool{}}
	tr := transactioner.Transactioner(lpg)

	uc := usecase{Transactioner: tr, repo: &repository{querier: lpg}}
	if err := uc.operationThatMustBeAtomicAndLazy(ctx); err != nil {
		panic(err)
	}
}

func (uc *usecase) operationThatMustBeAtomicAndLazy(ctx context.Context) (dst error) {
	tx, tctx, err := uc.Transaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Execute(&dst)

	tx.Add()
	_ = uc.repo.op0(tctx)

	// ... external calls

	tx.Add()
	_ = uc.repo.op1(tctx)

	println("END")

	return
}

type mockedPool struct{}

func (p *mockedPool) Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	return
}

func (p *mockedPool) Query(ctx context.Context, sql string, args ...interface{}) (rows pgx.Rows, err error) {
	return
}

func (p *mockedPool) QueryRow(ctx context.Context, sql string, args ...interface{}) (row pgx.Row) {
	return
}

func (p *mockedPool) Begin(ctx context.Context) (pg.Tx, error) {
	return p, nil
}

func (p *mockedPool) Commit(context.Context) error {
	return nil
}

func (p *mockedPool) Rollback(context.Context) error {
	return nil
}

type usecase struct {
	transactioner.Transactioner
	repo *repository
}

type repository struct {
	querier pg.Querier
}

func (r *repository) op0(ctx context.Context) error {
	_, err := r.querier.Exec(ctx, "SELECT 0;")
	return err
}

func (r *repository) op1(ctx context.Context) error {
	_, err := r.querier.Exec(ctx, "SELECT 1;")
	return err
}
