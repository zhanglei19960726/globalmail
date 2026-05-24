package outboxrelay

import (
	"context"
	"log"
	"time"

	"globalmail/domain/globalmail"
)

type Worker struct {
	relay    *globalmail.OutboxRelay
	limit    int
	interval time.Duration
	logger   *log.Logger
}

func NewWorker(relay *globalmail.OutboxRelay, limit int, interval time.Duration, logger *log.Logger) *Worker {
	if limit <= 0 {
		limit = 100
	}
	if interval <= 0 {
		interval = time.Second
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Worker{
		relay:    relay,
		limit:    limit,
		interval: interval,
		logger:   logger,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		if err := w.flush(ctx); err != nil {
			w.logger.Printf("outbox relay flush failed: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *Worker) flush(ctx context.Context) error {
	published, err := w.relay.Flush(ctx, w.limit)
	if err != nil {
		return err
	}
	if published > 0 {
		w.logger.Printf("outbox relay published %d events", published)
	}
	return nil
}
