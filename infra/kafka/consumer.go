package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"globalmail/config"
	"globalmail/domain/globalmail"

	kafkago "github.com/segmentio/kafka-go"
)

type GlobalMailConsumer struct {
	reader *kafkago.Reader
}

func NewGlobalMailConsumer(cfg config.KafkaConfig, groupID string) *GlobalMailConsumer {
	if groupID == "" {
		groupID = cfg.ConsumerGroup
	}
	if groupID == "" {
		groupID = "globalmail-playersrv"
	}
	return &GlobalMailConsumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: cfg.Brokers,
			Topic:   fmt.Sprintf("%s.global-mail-events", cfg.TopicPrefix),
			GroupID: groupID,
		}),
	}
}

func (c *GlobalMailConsumer) ReadGlobalMailChanged(ctx context.Context) (globalmail.GlobalMailChangedEvent, error) {
	message, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return globalmail.GlobalMailChangedEvent{}, err
	}
	var event globalmail.GlobalMailChangedEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return globalmail.GlobalMailChangedEvent{}, err
	}
	return event, nil
}

func (c *GlobalMailConsumer) Close() error {
	return c.reader.Close()
}
