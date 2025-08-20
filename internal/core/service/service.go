package service

import (
	"context"

	"github.com/niksmo/cloud-integration/internal/core/domain"
	"github.com/niksmo/cloud-integration/internal/core/port"
)

var _ port.PaymentSender = (*Service)(nil)

// generate messages for produce
// print messages after kafka

type Service struct {
	producer port.PaymentProducer
}

func New(p port.PaymentProducer) Service {
	return Service{p}
}

func (s Service) SendPayment(ctx context.Context, p domain.Payment) error {
	return nil
}
