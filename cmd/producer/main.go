package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Log struct {
	LogLevel    string `json:"log_level"`
	ServiceName string `json:"service_name"`
	Timestamp   string `json:"timestamp"`
	Message     string `json:"message"`
}

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger.Debug("starting main...")

	serviceName := flag.String("serviceName", "DEFAULT", "idk the usage of this argument")
	flag.Parse()

	if *serviceName == "" {
		log.Fatalf("serviceName flag is required")
	}

	msg := fmt.Sprintf("occured a problem in: %s service", *serviceName)
	day, month, year := time.Now().Date()
	timestamp := fmt.Sprintf("%d/%d/%d", day, month, year)

	data := Log{
		LogLevel:    "ERROR",
		ServiceName: *serviceName,
		Timestamp:   timestamp,
		Message:     msg,
	}

	b, err := json.Marshal(data)
	if err != nil {
		logger.Debug("failed to marshall the json log struct to json format")
		panic(err)
	}

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
		Value: b,
	}

	logger.Debug("producing message synchronously...")
	if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		logger.Error("record had a produce error", slog.Any("error", err))
	} else {
		logger.Debug("message produced successfully!")
	}

	logger.Debug("main finished")
}
