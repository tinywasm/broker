//go:build !wasm

package broker_test

import (
	"testing"
)

func TestBroker_Stdlib(t *testing.T) {
	t.Run("Consolidation", func(t *testing.T) {
		BrokerConsolidationShared(t)
	})

	t.Run("Flush", func(t *testing.T) {
		BrokerFlushShared(t)
	})

	t.Run("Clear", func(t *testing.T) {
		BrokerClearShared(t)
	})
}
