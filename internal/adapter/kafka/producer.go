package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/niksmo/cloud-integration/internal/core/domain"
	"github.com/niksmo/cloud-integration/internal/core/port"
	"github.com/niksmo/cloud-integration/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ port.PaymentProducer = (*Producer)(nil)

type ProducerClient interface {
	Close()
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

type ProducerOpt func(*producerOpts) error

func ProducerClientOpt(cl ProducerClient) ProducerOpt {
	return func(opts *producerOpts) error {
		if cl != nil {
			opts.cl = cl
			return nil
		}
		return errors.New("producer client is nil")
	}
}

func ProducerEncodeFnOpt(encodeFn func(v any) ([]byte, error)) ProducerOpt {
	return func(opts *producerOpts) error {
		if encodeFn != nil {
			opts.encodeFn = encodeFn
			return nil
		}
		return errors.New("producer encode func in nil")
	}
}

type producerOpts struct {
	cl       ProducerClient
	encodeFn func(v any) ([]byte, error)
}

type Producer struct {
	cl       ProducerClient
	encodeFn func(v any) ([]byte, error)
}

func NewProducer(opts ...ProducerOpt) Producer {
	const op = "NewProducer"

	var options producerOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			panic(err) //develop mistake
		}
	}
	return Producer{options.cl, options.encodeFn}
}

func (p Producer) Close() {
	const op = "Producer.Close"
	log := slog.With("op", op)
	log.Info("closing producer...")
	p.cl.Close()
	log.Info("producer is closed")
}

func (p Producer) ProducePayment(
	ctx context.Context, payment domain.Payment,
) error {
	const op = "Producer.ProducePayment"

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	r, err := p.createRecord(payment)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if err := p.produce(ctx, &r); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (p Producer) createRecord(
	payment domain.Payment,
) (kgo.Record, error) {
	const op = "Producer.createRecord"

	s := p.toSchema(payment)

	v, err := p.encodeFn(s)
	if err != nil {
		return kgo.Record{}, fmt.Errorf("%s: %w", op, err)
	}

	r := kgo.Record{Value: v}
	return r, nil
}

func (p Producer) toSchema(payment domain.Payment) schema.PaymentV1 {
	return schema.PaymentV1{
		ID:     payment.ID,
		Name:   payment.Name,
		Amount: payment.Amount,
	}
}

func (p Producer) produce(ctx context.Context, r *kgo.Record) error {
	const op = "Producer.produce"

	log := slog.With("op", op)

	start := time.Now()
	res := p.cl.ProduceSync(ctx, r)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	log.Info("message produced", "procDurMs", time.Since(start).Milliseconds())

	return nil
}
