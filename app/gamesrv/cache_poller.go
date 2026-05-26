package gamesrv

import (
	"context"
	"log"
	"time"
)

type GlobalMailCachePollRefresher interface {
	RefreshCache(ctx context.Context) error
}

func RunGlobalMailCachePoller(ctx context.Context, refresher GlobalMailCachePollRefresher, interval time.Duration, logger *log.Logger) error {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if logger == nil {
		logger = log.Default()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := refresher.RefreshCache(ctx); err != nil {
				logger.Printf("global mail cache version poll failed: %v", err)
			}
		}
	}
}
