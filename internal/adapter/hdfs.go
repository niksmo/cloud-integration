package adapter

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/google/uuid"
	"github.com/niksmo/cloud-integration/internal/core/domain"
	"github.com/niksmo/cloud-integration/internal/core/port"
)

var _ port.PaymentsStorage = (*HDFSStorage)(nil)

type HDFSOption func(*hdfsStorageOpts) error

func HDFSClientOpt(cl *hdfs.Client) HDFSOption {
	return func(hso *hdfsStorageOpts) error {
		if cl != nil {
			hso.cl = cl
			return nil
		}
		return errors.New("hdfs client is nil")
	}
}

type hdfsStorageOpts struct {
	cl *hdfs.Client
}

type HDFSStorage struct {
	cl *hdfs.Client
}

func NewHDFStorage(opts ...HDFSOption) HDFSStorage {
	const op = "NewHDFStorage"

	if len(opts) == 0 {
		panic(fmt.Errorf("%s: options not set", op))
	}

	var options hdfsStorageOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			panic(fmt.Errorf("%s: %w", op, err)) //develop mistake
		}
	}
	return HDFSStorage{options.cl}
}

func (s HDFSStorage) Close(onFall func(error)) {
	if err := s.cl.Close(); err != nil {
		onFall(err)
	}
}

func (s HDFSStorage) Save(ps []domain.Payment) error {
	const op = "HDFSStorage.Save"
	log := slog.With("op", op)

	filename := s.createFilepath()

	fw, err := s.cl.Create(filename)
	if err != nil {
		return fmt.Errorf("%s: failed to create file: %w", op, err)
	}

	fmt.Fprintf(fw, "%+v\n", ps)

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		<-timer.C
		err = fw.Close()
		if err != nil {
			if errors.Is(err, hdfs.ErrReplicating) {
				timer.Reset(1 * time.Second)
				continue
			}
			return fmt.Errorf("%s: failed to close file: %w", op, err)
		}
		break
	}

	log.Info("payments data saved successfully", "filename", filename)
	return nil
}

func (s HDFSStorage) createFilepath() string {
	return "/payments_" + uuid.NewString()
}
