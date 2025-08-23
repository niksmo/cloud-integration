package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/niksmo/cloud-integration/config"
	"github.com/niksmo/cloud-integration/internal/adapter"
	"github.com/niksmo/cloud-integration/internal/adapter/kafka"
	"github.com/niksmo/cloud-integration/internal/core/service"
	"github.com/niksmo/cloud-integration/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/sr"
)

func main() {
	sigCtx, cancel := signalContext()
	defer cancel()

	cfg := config.Load()

	initLogger(cfg.LogLevel)
	slog.Info("application is started")

	kafkaCl := createKafkaClient(cfg)
	serdeSR := createSerdeSR(sigCtx, cfg)

	producer := kafka.NewProducer(
		kafka.ProducerClientOpt(kafkaCl),
		kafka.ProducerEncodeFnOpt(serdeSR.Encode),
	)

	service := service.New(producer)

	consumer := kafka.NewConsumer(
		kafka.ConsumerClientOpt(kafkaCl),
		kafka.ConsumerReceiverOpt(service),
		kafka.ConsumerDecodeFnOpt(serdeSR.Decode),
	)

	paymentsGen := adapter.NewPaymentsGenerator(service, cfg.PaymentsGenTick)

	go consumer.Run(sigCtx)
	go paymentsGen.Run(sigCtx)

	<-sigCtx.Done()
	producer.Close()
	consumer.Close()
	slog.Info("application is stopped")
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(
		context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT,
	)
}

func initLogger(level slog.Leveler) {
	opts := &slog.HandlerOptions{Level: level}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, opts))
	slog.SetDefault(logger)
}

func createKafkaClient(cfg config.Config) *kgo.Client {
	const op = "Main.createKafkaClient"

	tlsConfig := createTLSConfig(cfg.Broker.CARootCert)

	auth := scram.Auth{
		User: cfg.Broker.User,
		Pass: cfg.Broker.Pass,
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Broker.SeedBrokers...),
		kgo.DialTLSConfig(tlsConfig),
		kgo.SASL(auth.AsSha512Mechanism()),
		// producer
		kgo.DefaultProduceTopicAlways(),
		kgo.DefaultProduceTopic(cfg.Broker.Topic),
		// consumer
		kgo.ConsumeTopics(cfg.Broker.Topic),
		kgo.ConsumerGroup(cfg.Broker.ConsumerGroup),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		die(op, err)
	}
	return cl
}

func createSerdeSR(
	ctx context.Context, cfg config.Config,
) *sr.Serde {
	const op = "Main.createSerdeSR"

	tlsConfig := createTLSConfig(cfg.Broker.CARootCert)

	cl, err := sr.NewClient(
		sr.URLs(cfg.Broker.SchemaRegistryURLs...),
		sr.DialTLSConfig(tlsConfig),
		sr.BasicAuth(cfg.Broker.User, cfg.Broker.Pass),
	)
	if err != nil {
		die(op, err)
	}

	ss, err := cl.CreateSchema(
		ctx, cfg.Broker.Topic+"-value", schema.PaymentSchemaV1,
	)
	if err != nil {
		die(op, err)
	}

	serde := new(sr.Serde)
	serde.Register(
		ss.ID,
		schema.PaymentV1{},
		sr.EncodeFn(schema.PaymentV1AvroEncodeFn()),
		sr.DecodeFn(schema.PaymentV1AvroDecodeFn()),
	)
	return serde
}

func createTLSConfig(CARootFilepath string) *tls.Config {
	const op = "Main.createTLSConfig"

	caRootPEM, err := os.ReadFile(CARootFilepath)
	if err != nil {
		die(op, err)
	}

	rootCAs := x509.NewCertPool()
	if ok := rootCAs.AppendCertsFromPEM(caRootPEM); !ok {
		err := fmt.Errorf("failed to parse CARootPEM: %q", CARootFilepath)
		die(op, err)
	}

	return &tls.Config{
		RootCAs:    rootCAs,
		ClientAuth: tls.NoClientCert,
	}
}

func die(op string, err error) {
	panic(fmt.Errorf("%s: %w", op, err))
}
