//go:build !integration

package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPayment(t *testing.T) {
	t.Run("AvroParseV1", func(t *testing.T) {
		require.NotPanics(t, func() {
			_ = PaymentV1Avro()
		})
	})
}
