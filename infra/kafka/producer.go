package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"globalmail/config"
	"globalmail/domain/globalmail"

	kafkago "github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *kafkago.Writer
}

func NewPublisher(cfg config.KafkaConfig, topic string) *Publisher {
	return &Publisher{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(cfg.Brokers...),
			Topic:        topic,
			Balancer:     &kafkago.Hash{},
			RequiredAcks: kafkago.RequireAll,
			Async:        false,
		},
	}
}

func NewGlobalMailPublisher(cfg config.KafkaConfig) *Publisher {
	return NewPublisher(cfg, fmt.Sprintf("%s.global-mail-events", cfg.TopicPrefix))
}

func (p *Publisher) PublishGlobalMailChanged(ctx context.Context, event globalmail.GlobalMailChangedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(fmt.Sprintf("%d", event.GlobalMailID)),
		Value: payload,
		Time:  time.Now().UTC(),
		Headers: []kafkago.Header{
			{Key: "event_type", Value: []byte(globalmail.EventTypeGlobalMailChanged)},
			{Key: "version", Value: []byte(fmt.Sprintf("%d", event.Version))},
		},
	})
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}
