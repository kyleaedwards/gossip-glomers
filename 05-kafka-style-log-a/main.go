package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendRequest struct {
	Type    string `json:"type"`
	Key     string `json:"key"`
	Message int    `json:"msg"`
}

type PollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollResponse struct {
	Type string             `json:"type"`
	Msgs map[string][][]int `json:"msgs"`
}

type CommitOffsetRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
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

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// TODO: Handle send

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": 0,
		})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body PollRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// TODO: Handle poll

		return n.Reply(msg, PollResponse{
			Type: "poll_ok",
			Msgs: map[string][][]int{
				"key": [][]int{
					[]int{1, 1},
					[]int{2, 2},
				},
			},
		})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body CommitOffsetRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// TODO: Handle commit offsets

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body CommitOffsetRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// TODO: Handle list committed offsets

		return n.Reply(msg, map[string]any{
			"type": "list_committed_offsets_ok",
			"offsets": map[string]int{
				"k1": 1000,
				"k2": 2000,
			},
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
