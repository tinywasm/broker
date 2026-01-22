package broker

import (
	"sync"

	"github.com/tinywasm/time"
)

// Item represents a consolidated group of data
type Item struct {
	Key  string
	Data [][]byte
}

// Broker handles batching of items for efficient processing
type Broker struct {
	mu          sync.Mutex
	queue       []Item
	batchWindow int
	timer       time.Timer
	tp          time.TimeProvider
	onFlush     func([]Item)
}

// New creates a new Broker with the specified batch window in milliseconds
func New(batchWindow int) *Broker {
	return &Broker{
		queue:       make([]Item, 0, 16),
		batchWindow: batchWindow,
		tp:          time.NewTimeProvider(),
	}
}

// SetOnFlush configures the callback for when the batch window expires
func (b *Broker) SetOnFlush(fn func([]Item)) {
	b.mu.Lock()
	b.onFlush = fn
	b.mu.Unlock()
}

// Enqueue adds data to the queue, consolidating by key
func (b *Broker) Enqueue(key string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Find existing item with same key to consolidate
	for i := range b.queue {
		if b.queue[i].Key == key {
			b.queue[i].Data = append(b.queue[i].Data, data)
			b.resetTimerLocked()
			return
		}
	}

	// New item
	b.queue = append(b.queue, Item{
		Key:  key,
		Data: [][]byte{data},
	})

	b.resetTimerLocked()
}

func (b *Broker) resetTimerLocked() {
	if b.timer != nil {
		b.timer.Stop()
	}
	b.timer = b.tp.AfterFunc(b.batchWindow, b.flush)
}

func (b *Broker) flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.queue) == 0 {
		return
	}

	// Create a copy for the callback
	items := make([]Item, len(b.queue))
	copy(items, b.queue)

	// Clear queue (keep capacity)
	b.queue = b.queue[:0]

	if b.onFlush != nil {
		b.onFlush(items)
	}
}

// FlushNow forces an immediate flush
func (b *Broker) FlushNow() {
	if b.timer != nil {
		b.timer.Stop()
	}
	b.flush()
}

// QueueLength returns the current number of unique keys in the queue
func (b *Broker) QueueLength() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue)
}

// Clear cleans the queue without triggering a flush
func (b *Broker) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	b.queue = b.queue[:0]
}
