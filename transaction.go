package transactioner

import (
	"errors"
	"fmt"
	"sync"
)

type Transaction struct {
	mu       sync.Mutex
	execute  []func() error
	data     map[interface{}]interface{}
	counter  uint
	executed bool
}

func (t *Transaction) Get(key interface{}) interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.data[key]
}

func (t *Transaction) Set(key, val interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.data[key] = val
}

func (t *Transaction) Add() {
	t.counter += 1
}

func (t *Transaction) Done() {
	t.counter -= 1
}

// RegisterExecute adds a new function to a queue that will run on `t.Execute` calls
func (t *Transaction) RegisterExecute(fun func() error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.execute = append(t.execute, fun)
}

/*
 Execute the transaction and assign any occurring error to a dst input variable.
 The API doesn't return the error in order to provide a single way to call Execute.
 And that way is through defer. E.g:

	func SomeAtomicFunction(...) (..., dst error) {
		tr := Transactioner(&SQLTransactioner{})
 		tx, tctx, er := tr.Transaction(ctx)
		if err != nil {
			return ..., err
		}
 		defer tx.Execute(&dst)

		// ... lots of code that could return err early using tctx as context.Context

		return
	}

*/
func (t *Transaction) Execute(dst *error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer func() { t.executed = true }()

	if t.executed {
		*dst = errors.New("transactioner: already executed")
	}

	for _, exe := range t.execute {
		if err := exe(); err != nil {
			println("FAILED")
			*dst = err
			return
		}
	}

	if t.counter != 0 {
		*dst = fmt.Errorf("transactioner: expected to execute %d more operations", t.counter)
	}
}
