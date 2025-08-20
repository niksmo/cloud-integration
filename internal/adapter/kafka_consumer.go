package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/niksmo/cloud-integration/internal/core/domain"
	"github.com/niksmo/cloud-integration/internal/core/port"
	"github.com/niksmo/cloud-integration/pkg/dialer"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type KafkaConsumerOpts struct {
	Receiver    port.PaymentReceiver
	SeedBrokers []string
	Topic       string
	Group       string
	CARootPath  string
	User        string
	Pass        string
}

type KafkaConsumer struct {
	cl       *kgo.Client
	service  port.PaymentReceiver
	errTimer *time.Timer
}

func NewKafkaConsumer(opts KafkaConsumerOpts) KafkaConsumer {
	const op = "NewKafkaConsumer"

	d, err := dialer.TLS(opts.CARootPath)
	if err != nil {
		panic(fmt.Errorf("%s: %w", op, err)) // develop mistake
	}

	auth := scram.Auth{
		User: opts.User,
		Pass: opts.Pass,
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(opts.SeedBrokers...),
		kgo.ConsumeTopics(opts.Topic),
		kgo.ConsumerGroup(opts.Group),
		kgo.DisableAutoCommit(),
		kgo.Dialer(d.DialContext),
		kgo.SASL(auth.AsSha512Mechanism()),
	)
	if err != nil {
		panic(fmt.Errorf("%s: %w", op, err)) // develop mistake
	}

	return KafkaConsumer{
		cl: cl, service: opts.Receiver, errTimer: time.NewTimer(0),
	}
}

func (c KafkaConsumer) Close() {
	const op = "KafkaConsumer.Close"
	log := slog.With("op", op)

	log.Info("closing consumer...")
	c.errTimer.Stop()
	c.cl.Close()
	log.Info("consumer is closed")
}

func (c KafkaConsumer) Run(ctx context.Context) {
	const op = "KafkaConsumer.Run"
	log := slog.With("op", op)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := c.consume(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					log.Info("context cancaled")
					continue
				}
				err = fmt.Errorf("%s: %w", op, err)
				log.Error("failed to consume messages", "err", err)
				c.slowDown()
			}
		}
	}
}

func (c KafkaConsumer) consume(ctx context.Context) error {
	const op = "KafkaConsumer.consume"

	fetches, err := c.pollFetches(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if fetches.Empty() {
		return nil
	}

	payments := c.toPayments(fetches)
	c.service.ReceivePayments(payments)
	return nil
}

func (c KafkaConsumer) pollFetches(ctx context.Context) (kgo.Fetches, error) {
	const op = "KafkaConsumer.pollFetches"

	fetches := c.cl.PollFetches(ctx)
	if err := fetches.Err0(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	err := c.handleErrs(fetches)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return fetches, nil
}

func (c KafkaConsumer) handleErrs(fetches kgo.Fetches) error {
	var errsData []string
	fetches.EachError(func(t string, p int32, err error) {
		if err != nil {
			errData := fmt.Sprintf(
				"topic %q partition %d: %q", t, p, err,
			)
			errsData = append(errsData, errData)
		}
	})

	if len(errsData) != 0 {
		return errors.New(strings.Join(errsData, "; "))
	}
	return nil
}

func (c KafkaConsumer) toPayments(
	fetches kgo.Fetches,
) []domain.Payment {
	const op = "KafkaConsumer.toPayments"
	log := slog.With("op", op)

	var payments []domain.Payment

	fetches.EachRecord(func(r *kgo.Record) {
		p, err := c.unmarshalValue(r.Value)
		if err != nil {
			err = fmt.Errorf("%s: %w", op, err)
			log.Error("failed to unmarshal value", "err", err)
			return
		}
		payments = append(payments, p)
	})
	return payments
}

func (c KafkaConsumer) unmarshalValue(v []byte) (domain.Payment, error) {
	const op = "KafkaConsumer.unmarshalValue"

	var p domain.Payment
	if err := json.Unmarshal(v, &p); err != nil {
		return domain.Payment{}, fmt.Errorf("%s: %w", op, err)
	}
	return p, nil
}

func (c KafkaConsumer) slowDown() {
	const timeout = 1 * time.Second
	c.errTimer.Reset(timeout)
	<-c.errTimer.C
}
