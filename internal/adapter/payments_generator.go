package adapter

import (
	"bytes"
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/niksmo/cloud-integration/internal/core/domain"
	"github.com/niksmo/cloud-integration/internal/core/port"
)

type PaymentsGenerator struct {
	service port.PaymentSender
	tick    time.Duration
	buf     bytes.Buffer
	a       []byte
}

func NewPaymentsGenerator(s port.PaymentSender, genTick time.Duration) *PaymentsGenerator {
	return &PaymentsGenerator{
		service: s,
		tick:    genTick,
		a:       []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
	}
}

func (g *PaymentsGenerator) Run(ctx context.Context) {
	const op = "PaymentsGenerator.Run"
	log := slog.With("op", op)

	ticker := time.NewTicker(g.tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p := g.createRandPayment()
			err := g.service.SendPayment(ctx, p)
			if err != nil {
				log.Error("failed to send payment", "err", err)
			}
		}
	}
}

func (g *PaymentsGenerator) createRandPayment() domain.Payment {
	p := domain.NewPayment(g.randName(), g.randAmount())
	return p
}

func (g *PaymentsGenerator) randName() (name string) {
	const nameSize = 5
	aSize := len(g.a)
	for range nameSize {
		g.buf.WriteByte(g.a[rand.IntN(aSize)])
	}
	name = g.buf.String()
	g.buf.Reset()
	return
}

func (g PaymentsGenerator) randAmount() float64 {
	whole := float64(rand.IntN(1_000) + 1)
	tenth := float64((rand.IntN(100) + 1)) / 100
	return whole + tenth
}
