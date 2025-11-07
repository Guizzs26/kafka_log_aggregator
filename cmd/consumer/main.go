package main

import (
	"context"
	"encoding/json"
	"fmt"

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

	seeds := []string{"localhost:9092"}
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics("logs"),
		kgo.ConsumerGroup("log_aggregator_group"),
		kgo.ClientID("consumer-id"),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	var log Log
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			if err := json.Unmarshal(record.Value, &log); err != nil {
				fmt.Printf("[WARNING]: failed to process message (poison pill?): %v\n", err)
				continue
			}
			fmt.Printf("[%s] [%s] [%s]: %s\n", log.LogLevel, log.ServiceName, log.Timestamp, log.Message)
		}
	}
}
