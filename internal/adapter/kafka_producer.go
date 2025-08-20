package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/niksmo/cloud-integration/internal/core/domain"
	"github.com/niksmo/cloud-integration/internal/core/port"
	"github.com/niksmo/cloud-integration/pkg/dialer"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var _ port.PaymentProducer = (*KafkaProducer)(nil)

type KafkaProducer struct {
	cl *kgo.Client
}

func NewKafkaProducer(
	seedBrokers []string, topic, caRootPath, user, pass string,
) KafkaProducer {
	const op = "NewKafkaProducer"

	d, err := dialer.TLS(caRootPath)
	if err != nil {
		panic(fmt.Errorf("%s: %w", op, err)) // develop mistake
	}

	auth := scram.Auth{
		User: user,
		Pass: pass,
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seedBrokers...),
		kgo.DefaultProduceTopicAlways(),
		kgo.DefaultProduceTopic(topic),
		kgo.Dialer(d.DialContext),
		kgo.SASL(auth.AsSha512Mechanism()),
	)
	if err != nil {
		panic(fmt.Errorf("%s: %w", op, err)) // develop mistake
	}

	return KafkaProducer{cl}
}

func (p KafkaProducer) Close() {
	const op = "KafkaProducer.Close"
	log := slog.With("op", op)
	log.Info("closing producer...")
	p.cl.Close()
	log.Info("producer is closed")
}

func (p KafkaProducer) ProducePayment(
	ctx context.Context, payment domain.Payment,
) error {
	const op = "KafkaProducer.ProducePayment"

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

func (p KafkaProducer) createRecord(
	payment domain.Payment,
) (kgo.Record, error) {
	const op = "KafkaProducer.createRecord"
	v, err := json.Marshal(payment)
	if err != nil {
		return kgo.Record{}, fmt.Errorf("%s: %w", op, err)
	}

	r := kgo.Record{Value: v}
	return r, nil
}

func (p KafkaProducer) produce(ctx context.Context, r *kgo.Record) error {
	const op = "KafkaProducer.produce"

	log := slog.With("op", op)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	start := time.Now()
	res := p.cl.ProduceSync(ctx, r)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	log.Info("message produced", "procDurMs", time.Since(start).Milliseconds())

	return nil
}
