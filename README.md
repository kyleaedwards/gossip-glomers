# Gossip Glomers

[Gossip Glomers](https://fly.io/dist-sys/) are a series of challenges to build familiarity with distributed systems created by [Fly.io](https://fly.io/) and [Kyle Kingsbury](https://aphyr.com/). This repo contains my solutions to these challenges.

1. [Echo](https://fly.io/dist-sys/1/)
2. [Unique ID Generator](https://fly.io/dist-sys/2/)
3. [Broadcast](https://fly.io/dist-sys/3a/)
4. [Grow-only Counter](https://fly.io/dist-sys/4/)
5. [Kafka-style Log](https://fly.io/dist-sys/5a/)
6. [Totally-available Transactions](https://fly.io/dist-sys/6a/)

## Echo

The first challenge is a simple echo worker to test the capabilities of the Maelstrom tool.

## Unique ID Generator

A UUID library may satisfy this requirement, however if IDs need to be serial, then a timestamp variation such as Snowflake may be appropriate.

## Broadcast

The first two parts ([3a](https://fly.io/dist-sys/3a/) and [3b](https://fly.io/dist-sys/3b/)) of this challenge establish a network of broadcast nodes by implementing a crude gossip protocol. Each node informs its neighbors of new incoming message IDs. If a node receives word of a message it's already received, it ignores it.

> **Note**: Gossiping is a common way to propagate information across a cluster when you don't need strong consistency guarantees.

The third part of this challenge involves making this system tolerant to network partitions by gracefully handling timeouts and failed broadcasts. My gut instinct is to switch from `Send`ing a broadcast to neighbors to using an RPC instead with a timeout context. On failure or timeout, we should queue up an action to retry this after a slight delay with some pseudo-random jitter to prevent piling up outgoing requests.

RPCs can be sent through the `SyncRPC()` method, so to avoid blocking, we can also set up a pool of worker goroutines to accept messages to send off of a channel. The `SyncRPC()` method also allows us to supply a `context.WithTimeout()`, which I've set to 100ms.

For [3d](https://fly.io/dist-sys/3d/), I introduced a batching method to periodically sync with neighbor nodes, and for [3e](https://fly.io/dist-sys/3e/) **(still in progress)**, I added some jitter to the timer for each node to prevent all of the messages getting sent at the same time. My best case was a medium stable latency of 1.668 seconds, which is still really high.

## Grow-only Counter

My first attempt was to use an append-only log, tracking inserted deltas with unique IDs in batches for each neighbor node. Once things were synched, we could calculate the total by traversing the log and summing up each delta once per UUID. This appeared to work, but I switched it to try using the Maelstrom KV store recommended in the challenge description. By using the `KV.CompareAndSwap()` method and retrying on failure, it eventually reached consistency.

> **TODO**: Investigate state-based conflict-free replicated data types as an alternative.

## Kafka-style Log

*In progress...*