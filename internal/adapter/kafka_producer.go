package adapter

import (
	"context"
	"fmt"
	"log/slog"

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

	// TODO: Delete
	err = cl.Ping(context.Background())
	if err != nil {
		slog.With("op", op).Warn("failed to ping cluster", "err", err)
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
	return nil
}
