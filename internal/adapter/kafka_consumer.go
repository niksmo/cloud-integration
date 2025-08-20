package adapter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

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
	const op = "KafkaConsumer.Close"
	log := slog.With("op", op)
	log.Info("closing consumer...")
	c.cl.Close()
	log.Info("consumer is closed")
}

func (c KafkaConsumer) Run(ctx context.Context) {
	const op = "KafkaConsumer.Run"
	log := slog.With("op", op)

	errTimeout := 1 * time.Second
	errTimer := time.NewTimer(errTimeout)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := c.cl.PollFetches(ctx)
			if err := fetches.Err0(); err != nil {
				if errors.Is(err, context.Canceled) {
					log.Info("context cancaled")
					continue
				}
				log.Error("unexpected error", "err", fmt.Errorf("%s: %w", op, err))
			}

			var errsData []string
			fetches.EachError(func(t string, p int32, err error) {
				if err != nil {
					errData := fmt.Sprintf(
						"topic %q partition %d err: %q",
						t, p, err,
					)
					errsData = append(errsData, errData)
				}
			})

			if len(errsData) != 0 {
				err := fmt.Errorf("%s: %s", op, strings.Join(errsData, "; "))
				log.Error("failed on poll fetches", "err", err)
				errTimer.Reset(errTimeout)
				<-errTimer.C
			}

		}
	}
}
