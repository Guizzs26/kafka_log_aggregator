package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger.Debug("starting main...")

	seeds := []string{"localhost:9092"}
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.DefaultProduceTopic("logs"),
		kgo.ClientID("producer-id"),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cl.Close()
	logger.Debug("successfully created kafka-franz client")

	acl := kadm.NewClient(cl)
	topicName := "logs"
	numPartitions := int32(3)
	replicationFactor := int16(1)

	logger.Debug("attempting to create topic", "topic", topicName)
	_, err = acl.CreateTopic(
		ctx,
		numPartitions,
		replicationFactor,
		nil,
		topicName,
	)
	if err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) {
			logger.Debug("topic already exists, proceeding...")
		} else {
			log.Fatalf("failed to create topic (unexpected error): %v", err)
		}
	} else {
		logger.Debug("topic created successfully")
	}

	record := &kgo.Record{
		Topic: "logs",
		Value: []byte("hello world"),
	}

	logger.Debug("producing message synchronously...")
	if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		logger.Error("record had a produce error", slog.Any("error", err))
	} else {
		logger.Debug("message produced successfylly!")
	}

	logger.Debug("main finished")
}
