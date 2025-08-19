package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/niksmo/cloud-integration/internal/adapter"
	"github.com/niksmo/cloud-integration/internal/core/service"
)

func main() {
	sigCtx, cancel := signalContext()
	defer cancel()

	initLogger()

	slog.Info("application is started")

	service := service.New()
	paymentsGen := adapter.NewPaymentsGenerator(service, 3*time.Second)

	go paymentsGen.Run(sigCtx)

	<-sigCtx.Done()
	slog.Info("application is stopped")
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(
		context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT,
	)
}

func initLogger() {
	opts := &slog.HandlerOptions{Level: slog.LevelDebug}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, opts))
	slog.SetDefault(logger)
}
