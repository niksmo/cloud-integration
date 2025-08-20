package adapter

import (
	"context"
	"fmt"

	"github.com/niksmo/cloud-integration/internal/core/port"
	"github.com/niksmo/cloud-integration/pkg/dialer"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type KafkaConsumer struct {
	cl      *kgo.Client
	service port.PaymentReceiver
}

func NewKafkaConsumer(
	receiver port.PaymentReceiver,
	seedBrokers []string, topic, group, caRootPath, user, pass string,
) KafkaConsumer {
	const op = "NewKafkaConsumer"

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
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.Dialer(d.DialContext),
		kgo.SASL(auth.AsSha512Mechanism()),
	)
	if err != nil {
		panic(fmt.Errorf("%s: %w", op, err)) // develop mistake
	}

	return KafkaConsumer{cl, receiver}
}

func (c KafkaConsumer) Close() {
	c.cl.Close()
}

func (c KafkaConsumer) Run(ctx context.Context) {
}
