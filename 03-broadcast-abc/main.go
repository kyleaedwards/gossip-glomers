package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Message int64  `json:"message"`
	Type    string `json:"type"`
	MsgId   *int64 `json:"msg_id"`
}

type BroadcastRPCPool struct {
	ch     chan BroadcastMessagePayload
	ctx    context.Context
	cancel context.CancelFunc
}

type BroadcastMessagePayload struct {
	dest             string
	broadcastMessage map[string]any
}

func createRPCPool(n *maelstrom.Node, poolSize int) *BroadcastRPCPool {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan BroadcastMessagePayload)

	for i := 0; i < poolSize; i += 1 {
		go func() {
			for {
				select {
				case msg := <-ch:
					{
						for {
							newCtx, _ := context.WithTimeout(ctx, 100*time.Millisecond)
							_, err := n.SyncRPC(newCtx, msg.dest, msg.broadcastMessage)
							if err == nil {
								break
							}
						}
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return &BroadcastRPCPool{
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
	}
}

func main() {
	n := maelstrom.NewNode()
	messages := make(map[int64]bool)
	var mutex sync.RWMutex

	broadcastClient := createRPCPool(n, 10)
	defer broadcastClient.cancel()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mutex.RLock()
		_, ok := messages[body.Message]
		mutex.RUnlock()
		if !ok {
			mutex.Lock()
			messages[body.Message] = true
			mutex.Unlock()
		}

		if !ok {
			broadcastMsg := map[string]any{
				"type":    "broadcast",
				"message": body.Message,
			}

			// Send broadcasts to all neighbors
			for _, neighborId := range n.NodeIDs() {
				if neighborId == n.ID() || neighborId == msg.Src {
					continue
				}

				broadcastClient.ch <- BroadcastMessagePayload{
					dest:             neighborId,
					broadcastMessage: broadcastMsg,
				}
			}
		}

		if body.MsgId != nil {
			// Update the message type to return back.
			response := map[string]any{
				"type": "broadcast_ok",
			}
			return n.Reply(msg, response)
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		//mutex.Lock()
		mutex.RLock()
		nums := make([]int64, len(messages))
		i := 0
		for k := range messages {
			nums[i] = k
			i += 1
		}
		//mutex.Unlock()
		mutex.RUnlock()

		// Update the message type to return back.
		response := map[string]any{
			"type":     "read_ok",
			"messages": nums,
		}

		return n.Reply(msg, response)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		response := map[string]any{
			"type": "topology_ok",
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
