package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddMessage struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type FetchMessage struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

type FetchRequest struct {
	Type string `json:"type"`
}

type Service struct {
	node  *maelstrom.Node
	mutex sync.RWMutex
	kv    *maelstrom.KV
}

const timeout = time.Millisecond * 750

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	s := Service{
		node: n,
		kv:   kv,
	}
	s.mutex.Lock()

	n.Handle("init", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx, _ := context.WithTimeout(context.Background(), timeout)
		_, err := kv.ReadInt(ctx, "count")
		if err != nil {
			kv.Write(ctx, "count", 0)
		}
		s.mutex.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "init_ok",
		})
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body AddMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mutex.Lock()
		for {
			ctx, _ := context.WithTimeout(context.Background(), timeout)
			val, err := kv.ReadInt(ctx, "count")
			if err != nil {
				continue
			}
			ctx, _ = context.WithTimeout(context.Background(), timeout)
			err = kv.CompareAndSwap(ctx, "count", val, val+body.Delta, false)
			if err != nil {
				continue
			}
			break
		}
		s.mutex.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx, _ := context.WithTimeout(context.Background(), timeout)
		s.mutex.RLock()
		val, err := kv.ReadInt(ctx, "count")
		s.mutex.RUnlock()
		if err != nil {
			return err
		}

		// Update the message type to return back.
		response := map[string]any{
			"type":  "read_ok",
			"value": val,
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
