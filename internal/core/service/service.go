package service

import (
	"context"

	"github.com/niksmo/cloud-integration/internal/core/domain"
)

// generate messages for produce
// print messages after kafka

type Service struct{}

func New() Service {
	return Service{}
}

func (s Service) SendPayment(ctx context.Context, p domain.Payment) error {
	return nil
}
