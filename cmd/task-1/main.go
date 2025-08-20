package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/niksmo/cloud-integration/config"
	"github.com/niksmo/cloud-integration/internal/adapter"
	"github.com/niksmo/cloud-integration/internal/core/service"
)

func main() {
	sigCtx, cancel := signalContext()
	defer cancel()

	cfg := config.Load()

	initLogger(cfg.LogLevel)

	slog.Info("application is started")

	producer := adapter.NewKafkaProducer(
		cfg.Broker.SeedBrokers,
		cfg.Broker.Topic,
		cfg.Broker.CARootCert,
		cfg.Broker.User,
		cfg.Broker.Pass,
	)

	service := service.New(producer)
	paymentsGen := adapter.NewPaymentsGenerator(service, cfg.PaymentsGenTick)

	go paymentsGen.Run(sigCtx)

	<-sigCtx.Done()
	producer.Close()
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
