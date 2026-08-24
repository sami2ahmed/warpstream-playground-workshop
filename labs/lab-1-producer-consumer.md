# Lab 1: Producers, Consumers & Fundamentals

## Learning Objectives

By the end of this lab, you will:

- Produce and consume messages using `kcat`.
- Understand how **Consumer Groups** enable parallel processing.
- Master offset tracking and committing.
- Learn why **WarpStream performance tuning** differs from traditional Kafka (Batching is key!).

## Concept Introduction

In WarpStream (as in Kafka), data flow relies on Producers, Topics, and Consumers.

- **Producers**: Write data to topics.
- **Topics**: Logical categories for messages, divided into **partitions**.
- **Consumers**: Read data. They can work alone or in **groups**.

### Data Flow Architecture

```
+----------------+          +--------------------+          +----------------+
| Producer (kcat)|          | Topic (WarpStream) |          | Consumer (kcat)|
+----------------+          +--------------------+          +----------------+
        |                             |                             |
        |--- Produce Message -------->|                             |
        |                             |                             |
        |                             |-- Push Message ------------>|
        |                             |                             |
        |                             |<-- Commit Offset (Ack) -----|
```

### Consumer Groups

Consumer Groups allow you to scale processing. A group is a team of consumers that share the work of reading a topic.

```
Topic: orders                 Consumer Group: order-processors
+-------------+              +------------+
| Partition 0 | -----------> | Consumer A |
+-------------+              +------------+

+-------------+              +------------+
| Partition 1 | -----------> | Consumer B |
+-------------+              +------------+

+-------------+              +------------+
| Partition 2 | -----------> | Consumer C |
+-------------+              +------------+
```

---

## Step 1: Produce Messages

Use `kcat` in **Producer Mode** (`-P`) to send data.

### Simple Message

```bash
echo "my first message" | kcat -b localhost:9092 -t messages -P
```

### Multiple Messages

```bash
echo -e "message 1\nmessage 2\nmessage 3" | kcat -b localhost:9092 -t messages -P
```

### JSON Messages

```bash
echo '{"user": "alice", "action": "login"}' | kcat -b localhost:9092 -t events -P
echo '{"user": "bob", "action": "purchase", "amount": 99.99}' | kcat -b localhost:9092 -t events -P
```

### Keyed Messages

Keys ensure that messages with the same key always go to the same partition (ordering guarantee).

To see this in action you need a topic with **more than one partition**. Topics that WarpStream auto-creates when you produce to them have a single partition, so every key would land on partition 0 and the routing would be invisible. Create the topic explicitly instead, using the WarpStream CLI:

```bash
warpstream kcmd --type create-topic --topic keyed --partition-count 6
```

> **Note**: If you already produced to `keyed` earlier, it exists with 1 partition. Delete it first, then re-run the create command above:
>
> ```bash
> warpstream kcmd --type delete-topic --topic keyed --hard-delete
> ```

Confirm it has 6 partitions:

```bash
kcat -b localhost:9092 -L -t keyed
```

Now produce one message per key. The `-K:` flag tells `kcat` that `:` separates the key from the value:

```bash
# Syntax: key:value
for k in user-1 user-2 user-3 user-4 user-5 user-6; do
  echo "$k:order from $k" | kcat -b localhost:9092 -t keyed -P -K:
done
```

Then send two more messages reusing an existing key, so you can check they follow the first one:

```bash
echo "user-1:order 2 from user-1" | kcat -b localhost:9092 -t keyed -P -K:
echo "user-1:order 3 from user-1" | kcat -b localhost:9092 -t keyed -P -K:
```

## Step 2: Consume Messages

Use `kcat` in **Consumer Mode** (`-C`) to read data.

### Consume from Beginning

```bash
kcat -b localhost:9092 -t messages -C -o beginning -e
```

- `-o beginning`: Start at the oldest message.
- `-e`: Exit when the end of the topic is reached.

### Consume with Metadata

View partition, offset, and key information for the `keyed` topic you created in Step 1. Sorting the output groups each partition together so the key routing is easy to read:

```bash
kcat -b localhost:9092 -t keyed -C -o beginning -e \
  -f 'Partition: %p | Offset: %o | Key: %k | Value: %s\n' 2>/dev/null | sort
```

- `2>/dev/null`: Hides `kcat` progress messages so only records are shown.
- `sort`: Groups records by partition. Drop it to see the arrival order instead.

**Expected Output:**

```
Partition: 0 | Offset: 0 | Key: user-3 | Value: order from user-3
Partition: 2 | Offset: 0 | Key: user-1 | Value: order from user-1
Partition: 2 | Offset: 1 | Key: user-1 | Value: order 2 from user-1
Partition: 2 | Offset: 2 | Key: user-1 | Value: order 3 from user-1
Partition: 3 | Offset: 0 | Key: user-4 | Value: order from user-4
Partition: 3 | Offset: 1 | Key: user-5 | Value: order from user-5
Partition: 3 | Offset: 2 | Key: user-6 | Value: order from user-6
Partition: 4 | Offset: 0 | Key: user-2 | Value: order from user-2
```

Two things to notice:

1. **Keys are spread across partitions.** The client hashes each key and divides by the partition count, so the six keys land on different partitions. Some partitions get several keys and some get none, which is normal for hash-based routing with so few keys.
2. **All three `user-1` messages are on the same partition**, at consecutive offsets. That is the ordering guarantee: a key's messages are always read back in the order they were written. Kafka only orders messages *within* a partition, so `user-1` and `user-2` have no ordering relative to each other.

Because the hash is deterministic, re-running this lab with 6 partitions puts the same keys on the same partitions. Change the partition count and the mapping changes, which is why repartitioning a topic breaks existing key-to-partition assignments.

## Step 3: Consumer Groups & Offsets

When you specify a group ID (`-G`), the broker manages offsets for you, ensuring each message is processed only once per group.

1. **Start a Consumer Group** (leave it running):
  ```bash
  kcat -b localhost:9092 -G my-group -e -o beginning orders
  ```
2. **Produce 10 messages** in a second terminal:
  ```bash
    for i in {1..10}; do
      echo "order-$i" | kcat -b localhost:9092 -t orders -P
    done
  ```
    *Watch the consumer terminal. You should see each message land as it is produced. Press* `Ctrl+C` *in the consumer terminal to stop.*
3. **Verify Offset Tracking:**
  Produce 5 more messages:

```bash
  for i in {10..15}; do
    echo "order-$i" | kcat -b localhost:9092 -t orders -P
  done
```

  Resume the consumer group:

```bash
kcat -b localhost:9092 -G my-group -e orders
```

  *Result: You should only see messages 11-15. The group "remembered" it had already processed 1-10.*

## Step 4: Performance Tuning (WarpStream Specific)

WarpStream writes directly to Object Storage (S3), which has higher latency than local SSDs but infinite scale. To get high performance, you [must tune the clients](https://docs.warpstream.com/warpstream/kafka/configure-kafka-client/tuning-for-performance).

### The Golden Rule: Batching

Sending one message at a time is slow. Sending a bucket of 1,000 messages is fast.


| Setting       | Recommendation | Why?                                     |
| ------------- | -------------- | ---------------------------------------- |
| `linger.ms`   | `100`          | Wait 100ms to accumulate a larger batch. |
| `batch.size`  | `1MB`          | Fill larger buckets before sending.      |
| `compression` | `lz4`          | Reduce network bandwidth.                |


### Performance Comparison

**Scenario A: Default (Slow)**
Sending messages one-by-one (simulates `linger.ms=0`). We only send 10 messages here so the demo finishes quickly.

```bash
echo "Sending messages one-by-one..."
time for i in {1..10}; do
  echo "message-$i" | kcat -b localhost:9092 -t perf-demo -P
done
```

**Scenario B: Optimized (Fast)**
Sending messages in a stream allows the client to batch them efficiently. This run uses 1–100 messages (10x Scenario A) so the batching win is easier to see even with more work.

```bash
echo "Sending messages in batches..."
time ( for i in {1..100}; do
  echo "message-batch-$i"
done | kcat -b localhost:9092 -t perf-demo -P \
  -X linger.ms=100 \
  -X batch.size=1048576 \
  -X compression.codec=lz4)
```

*Result: Scenario B should still finish faster than Scenario A despite sending 100 messages instead of 10, because* `kcat` *can batch the input stream.*

Proceed to [Lab 2: WarpStream Agent Groups](lab-2-agent-groups.md) to explore advanced deployment topologies.