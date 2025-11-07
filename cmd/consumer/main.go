package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

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

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			fmt.Println(string(record.Value), "testing")
		}
	}
}
