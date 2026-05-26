package gamesrv

import (
	"context"
	"errors"

	"globalmail/domain/globalmail"
)

type GlobalMailChangedReader interface {
	ReadGlobalMailChanged(ctx context.Context) (globalmail.GlobalMailChangedEvent, error)
}

type GlobalMailCacheRefresher interface {
	CacheVersion() int64
	ForceRefreshCache(ctx context.Context) error
}

type EventConsumer struct {
	reader    GlobalMailChangedReader
	refresher GlobalMailCacheRefresher
}

func NewEventConsumer(reader GlobalMailChangedReader, refresher GlobalMailCacheRefresher) *EventConsumer {
	return &EventConsumer{reader: reader, refresher: refresher}
}

func (c *EventConsumer) Run(ctx context.Context) error {
	for {
		event, err := c.reader.ReadGlobalMailChanged(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		if err := c.Handle(ctx, event); err != nil {
			return err
		}
	}
}

func (c *EventConsumer) Handle(ctx context.Context, event globalmail.GlobalMailChangedEvent) error {
	if event.Version > 0 && event.Version <= c.refresher.CacheVersion() {
		return nil
	}
	return c.refresher.ForceRefreshCache(ctx)
}
