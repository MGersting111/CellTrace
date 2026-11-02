package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

type QualityHold struct {
	HoldID     string    `json:"hold_id"`
	Serial     string    `json:"serial"`
	ReasonCode string    `json:"reason_code"`
	PlacedAt   time.Time `json:"placed_at"`
}

type Store struct {
	mu    sync.RWMutex // schützt holds
	holds []QualityHold
}

func (s *Store) Add(h QualityHold) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holds = append(s.holds, h)
}

func (s *Store) All() []QualityHold {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]QualityHold(nil), s.holds...) // Kopie, damit der Aufrufer außerhalb des Locks liest
}

var header sr.ConfluentHeader

func toQualityHold(event map[string]any) (QualityHold, bool) {
	payload, ok := event["payload"].(map[string]any)
	if !ok {
		return QualityHold{}, false
	}
	placed, ok := payload["io.celltrace.events.QualityHoldPlacedPayload"].(map[string]any)
	if !ok {
		return QualityHold{}, false // anderer Union-Zweig
	}

	holdID, _ := placed["hold_id"].(string)
	serial, _ := placed["serial"].(string)
	reason, _ := placed["reason_code"].(string)
	placedAt, _ := event["occurred_at"].(time.Time)

	return QualityHold{
		HoldID:     holdID,
		Serial:     serial,
		ReasonCode: reason,
		PlacedAt:   placedAt,
	}, true
}

func decode(ctx context.Context, registry *sr.Client, cache map[int]avro.Schema, value []byte) (map[string]any, error) {
	id, avroBytes, err := header.DecodeID(value)
	if err != nil {
		return nil, fmt.Errorf("header: %w", err)
	}

	schema, ok := cache[id]
	if !ok {
		s, err := registry.SchemaByID(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("schema %d: %w", id, err)
		}
		schema, err = avro.Parse(s.Schema)
		if err != nil {
			return nil, fmt.Errorf("parse: %w", err)
		}
		cache[id] = schema
	}

	var event map[string]any
	if err := avro.Unmarshal(schema, avroBytes, &event); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return event, nil
}

func consume(ctx context.Context, client *kgo.Client, registry *sr.Client,
	cache map[int]avro.Schema, store *Store) {
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		for _, e := range fetches.Errors() {
			log.Printf("fetch error %s/%d: %v", e.Topic, e.Partition, e.Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			event, err := decode(ctx, registry, cache, r.Value)
			if err != nil {
				log.Printf("decode offset=%d: %v", r.Offset, err)
				return // diese Nachricht überspringen
			}
			hold, ok := toQualityHold(event)
			if !ok {
				log.Printf("offset=%d: kein QualityHoldPlaced, übersprungen", r.Offset)
				return
			}
			fmt.Printf("%+v\n", hold)
			store.Add(hold)
		})
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup("terminal-backend"),
		kgo.ConsumeTopics("mes.quality"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	defer client.Close()

	registry, err := sr.NewClient(sr.URLs("http://localhost:8081"))
	if err != nil {
		log.Fatalf("registry: %v", err)
	}
	cache := map[int]avro.Schema{}

	store := &Store{}

	go consume(ctx, client, registry, cache, store)

	http.HandleFunc("/holds", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(store.All()); err != nil {
			log.Printf("encode: %v", err)
		}
	})
	log.Fatal(http.ListenAndServe(":8080", nil))

}
