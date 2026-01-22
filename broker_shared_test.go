package broker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/tinywasm/broker"
)

func BrokerConsolidationShared(t *testing.T) {
	t.Run("Consolidate Same Key", func(t *testing.T) {
		b := broker.New(500)

		b.Enqueue("key1", []byte("A"))
		b.Enqueue("key1", []byte("B"))
		b.Enqueue("key1", []byte("C"))

		if b.QueueLength() != 1 {
			t.Errorf("expected 1 item, got %d", b.QueueLength())
		}
	})

	t.Run("Different Keys Not Consolidated", func(t *testing.T) {
		b := broker.New(500)

		b.Enqueue("key1", []byte("A"))
		b.Enqueue("key2", []byte("B"))
		b.Enqueue("key3", []byte("C"))

		if b.QueueLength() != 3 {
			t.Errorf("expected 3 items, got %d", b.QueueLength())
		}
	})
}

func BrokerFlushShared(t *testing.T) {
	t.Run("Flush After BatchWindow", func(t *testing.T) {
		b := broker.New(50)

		var flushedItems []broker.Item
		var wg sync.WaitGroup
		wg.Add(1)

		b.SetOnFlush(func(items []broker.Item) {
			flushedItems = items
			wg.Done()
		})

		b.Enqueue("key1", []byte("test"))

		// Wait for flush (with timeout)
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			if len(flushedItems) == 0 {
				t.Error("expected flush items")
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("timeout waiting for flush")
		}

		if b.QueueLength() != 0 {
			t.Error("queue should be empty after flush")
		}
	})

	t.Run("FlushNow Forces Immediate Flush", func(t *testing.T) {
		b := broker.New(5000)

		var flushed bool
		b.SetOnFlush(func(items []broker.Item) {
			flushed = true
		})

		b.Enqueue("key1", []byte("test"))
		b.FlushNow()

		if !flushed {
			t.Error("expected immediate flush")
		}

		if b.QueueLength() != 0 {
			t.Error("queue should be empty")
		}
	})

	t.Run("Empty Queue No Flush", func(t *testing.T) {
		b := broker.New(5000)

		flushed := false
		b.SetOnFlush(func(items []broker.Item) {
			flushed = true
		})

		b.FlushNow()

		if flushed {
			t.Error("should not flush empty queue")
		}
	})
}

func BrokerClearShared(t *testing.T) {
	t.Run("Clear Removes All", func(t *testing.T) {
		b := broker.New(5000)

		b.Enqueue("key1", []byte("A"))
		b.Enqueue("key2", []byte("B"))

		if b.QueueLength() != 2 {
			t.Fatal("setup failed")
		}

		b.Clear()

		if b.QueueLength() != 0 {
			t.Error("queue should be empty after clear")
		}
	})
}
