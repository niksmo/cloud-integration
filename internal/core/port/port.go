package port

import (
	"context"

	"github.com/niksmo/cloud-integration/internal/core/domain"
)

type PaymentSender interface {
	SendPayment(context.Context, domain.Payment) error
}

type PaymentProducer interface {
	ProducePayment(context.Context, domain.Payment) error
}

type PaymentReceiver interface {
	ReceivePayments([]domain.Payment)
}
