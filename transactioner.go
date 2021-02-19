package transactioner

import (
	"context"
)

type ctxKey string

const txKey = ctxKey("tx")

type Transactioner interface {
	Transaction(context.Context) (*Transaction, context.Context, error)
}

// TransactionContext either creates a new transaction or returns a transaction already in ctx
// returning a context with a transaction in it
func TransactionContext(ctx context.Context) (*Transaction, context.Context) {
	if tx := GetTransaction(ctx); tx != nil {
		return tx, ctx
	}
	tx := &Transaction{data: map[interface{}]interface{}{}}
	return tx, context.WithValue(ctx, txKey, tx)
}

func GetTransaction(ctx context.Context) *Transaction {
	txI := ctx.Value(txKey)
	tx, ok := txI.(*Transaction)
	if !ok {
		return nil
	}
	return tx
}
