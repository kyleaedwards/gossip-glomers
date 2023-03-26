package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Message int    `json:"message"`
	Type    string `json:"type"`
	MsgId   *int64 `json:"msg_id"`
}

type SyncMessage struct {
	Message string `json:"message"`
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
	wg               *sync.WaitGroup
}

type TopologyMap map[string][]string

type TopologyMessage struct {
	Topology TopologyMap `json:"topology"`
}

type Service struct {
	node       *maelstrom.Node
	client     *BroadcastRPCPool
	messages   map[int]bool
	topology   TopologyMap
	msgMutex   sync.RWMutex
	batchIds   []string
	batchMutex sync.Mutex
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
							newCtx, _ := context.WithTimeout(ctx, 2000*time.Millisecond)
							_, err := n.SyncRPC(newCtx, msg.dest, msg.broadcastMessage)
							if err == nil {
								msg.wg.Done()
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

func (s *Service) runSync() {
	s.batchMutex.Lock()
	defer s.batchMutex.Unlock()

	if len(s.batchIds) == 0 {
		return
	}

	broadcastMsg := map[string]any{
		"type":    "sync",
		"message": strings.Join(s.batchIds, ","),
	}

	if neighbors, ok := s.topology[s.node.ID()]; ok {
		var wg sync.WaitGroup
		for _, neighborId := range neighbors {
			wg.Add(1)
			s.client.ch <- BroadcastMessagePayload{
				dest:             neighborId,
				broadcastMessage: broadcastMsg,
				wg:               &wg,
			}
		}
		wg.Wait()
	}
}

func main() {
	n := maelstrom.NewNode()

	broadcastClient := createRPCPool(n, 10)
	defer broadcastClient.cancel()

	s := Service{
		node:     n,
		client:   broadcastClient,
		messages: make(map[int]bool),
		topology: make(TopologyMap),
		batchIds: make([]string, 0),
	}

	n.Handle("sync", func(msg maelstrom.Message) error {
		var body SyncMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := strings.Split(body.Message, ",")
		ids := make([]int, 0, len(msgs))
		batchIds := make([]string, 0, len(msgs))

		s.msgMutex.RLock()
		for _, msg := range msgs {
			id, err := strconv.Atoi(msg)
			if err == nil {
				_, ok := s.messages[id]
				if !ok {
					ids = append(ids, id)
					batchIds = append(batchIds, msg)
				}
			}
		}
		s.msgMutex.RUnlock()

		if len(ids) > 0 {
			s.msgMutex.Lock()
			for _, id := range ids {
				s.messages[id] = true
			}
			s.msgMutex.Unlock()
			s.batchMutex.Lock()
			s.batchIds = append(s.batchIds, batchIds...)
			s.batchMutex.Unlock()
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

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.msgMutex.RLock()
		_, ok := s.messages[body.Message]
		s.msgMutex.RUnlock()

		if !ok {
			s.msgMutex.Lock()
			s.messages[body.Message] = true
			s.msgMutex.Unlock()
			s.batchMutex.Lock()
			s.batchIds = append(s.batchIds, strconv.Itoa(body.Message))
			s.batchMutex.Unlock()
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

		s.msgMutex.RLock()
		nums := make([]int, len(s.messages))
		i := 0
		for k := range s.messages {
			nums[i] = k
			i += 1
		}
		s.msgMutex.RUnlock()

		// Update the message type to return back.
		response := map[string]any{
			"type":     "read_ok",
			"messages": nums,
		}

		return n.Reply(msg, response)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		s.topology = body.Topology

		// Update the message type to return back.
		response := map[string]any{
			"type": "topology_ok",
		}

		return n.Reply(msg, response)
	})

	ticker := time.NewTicker(500 * time.Millisecond)
	quit := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				s.runSync()
			case <-quit:
				return
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
		quit <- struct{}{}
	}
}
