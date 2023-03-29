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
		kv.Write(ctx, n.ID(), 0)
		s.mutex.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "init_ok",
		})
	})

	n.Handle("fetch", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mutex.RLock()
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		val, err := kv.ReadInt(ctx, n.ID())
		s.mutex.RUnlock()
		if err != nil {
			return err
		}

		log.Printf("Fetching %s: %d\n", n.ID(), val)

		return n.Reply(msg, map[string]any{
			"type":  "fetch_ok",
			"value": val,
		})
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body AddMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.mutex.Lock()
		ctx, _ := context.WithTimeout(context.Background(), timeout)
		val, err := kv.ReadInt(ctx, n.ID())
		if err != nil {
			return err
		}
		ctx, _ = context.WithTimeout(context.Background(), timeout)
		kv.Write(ctx, n.ID(), val+body.Delta)
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

		var total int
		var mut sync.Mutex
		var wg sync.WaitGroup
		for _, nodeId := range n.NodeIDs() {
			wg.Add(1)
			if nodeId == n.ID() {
				go func() {
					ctx, _ := context.WithTimeout(context.Background(), timeout)
					val, err := kv.ReadInt(ctx, nodeId)
					if err != nil {
						return
					}
					mut.Lock()
					total += val
					mut.Unlock()
					wg.Done()
				}()
			} else {
				go func() {
					ctx, _ := context.WithTimeout(context.Background(), timeout)
					res, err := s.node.SyncRPC(ctx, nodeId, FetchRequest{
						Type: "fetch",
					})
					if err != nil {
						return
					}
					var body FetchMessage
					if err = json.Unmarshal(res.Body, &body); err != nil {
						return
					}
					mut.Lock()
					total += body.Value
					mut.Unlock()
					wg.Done()
				}()
			}
		}
		wg.Wait()

		// Update the message type to return back.
		response := map[string]any{
			"type":  "read_ok",
			"value": total,
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
