package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/niksmo/cloud-integration/internal/core/domain"
	"github.com/niksmo/cloud-integration/internal/core/port"
)

var _ port.PaymentSender = (*Service)(nil)
var _ port.PaymentReceiver = (*Service)(nil)

type Service struct {
	producer port.PaymentProducer
	storage  port.PaymentsStorage
}

func New(p port.PaymentProducer, s port.PaymentsStorage) Service {
	return Service{p, s}
}

func (s Service) SendPayment(ctx context.Context, p domain.Payment) error {
	const op = "Service.SendPayment"
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err := s.producer.ProducePayment(ctx, p)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (s Service) ReceivePayments(ps []domain.Payment) {
	const op = "Service.ReceivePayment"
	log := slog.With("op", op)
	for _, p := range ps {
		log.Info("receive payment", "payment", p)
	}

	s.savePayments(ps)
}

func (s Service) savePayments(ps []domain.Payment) {
	const op = "Service.storePayments"
	log := slog.With("op", op)
	if err := s.storage.Save(ps); err != nil {
		log.Error("failed to save payments", "err", err)
	}
}
