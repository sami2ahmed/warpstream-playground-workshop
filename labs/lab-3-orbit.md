# Lab 3: WarpStream Orbit (Cluster Linking)

## Learning Objectives

By the end of this lab, you will:
- Understand how Orbit enables zero-downtime migrations.
- Replicate data from an external Kafka cluster to WarpStream.
- Verify offset preservation between source and destination.

## Concept Introduction

**Orbit** is WarpStream's cluster linking technology. It allows a WarpStream cluster to act as a read-replica of another Kafka cluster (the "Source").

This is critical for migrations:
1.  **Connect** WarpStream to your existing Kafka.
2.  **Replicate** all data and offsets automatically.
3.  **Migrate** consumers to WarpStream at your own pace.

### Architecture

```
+-------------------------------+       +-------------------------------+
| Legacy Kafka Cluster :9093    |       | WarpStream Cluster :9092      |
|                               |       |                               |
|  [Topic: orders] --(Replicate)--------> [Orbit Agent] --> [Topic: orders]
|                               |       |       ^                       |
|  [Topic: payments] -(Replicate)-------|       |                       |
|                               |       |       +-----> [Topic: payments]
+-------------------------------+       +-------------------------------+
```

---

# Migration Simulation

Since we don't have a production Kafka cluster handy, we will spin up a separate local Kafka container to act as the "Legacy Source".

## Prerequisites: Docker

Lab 3 needs Docker Desktop running before you start the source Kafka cluster.

**macOS install (if Docker is missing):**
```bash
brew install --cask docker
```

Then open **Docker** from Applications and wait until it says it is running.

**Quick check:**
```bash
docker info >/dev/null && echo "Docker is ready"
```

## Step 1: Start Source Kafka Cluster

We need a second Kafka cluster running on a different port (9093).

```bash
docker compose -f docker/docker-compose-lab3.yml up -d
```

**Verify connectivity to the "Legacy" cluster:**
```bash
kcat -b localhost:9093 -L
```
*You should see a broker at `localhost:9093`.*

## Step 2: Simulate "Legacy" Data

Let's generate some traffic on the source cluster.

1.  **Produce Orders:**
    ```bash
    for i in {1..20}; do
      echo "order-$i" | kcat -b localhost:9093 -t orders -P
    done
    ```

2.  **Produce Payments:**
    ```bash
    for i in {1..10}; do
      echo "payment-$i" | kcat -b localhost:9093 -t payments -P
    done
    ```

3.  **Verify Data:**
    ```bash
    kcat -b localhost:9093 -t orders -C -o beginning -e
    ```

## Step 3: Orbit Replication

Follow the [orbit documentation](https://docs.warpstream.com/warpstream/kafka/orbit)

### Spot-check replication on WarpStream

The Console UI can lag briefly if you move quickly. As a fallback, confirm the `orders` and `payments` topics showed up on the WarpStream broker:

```bash
kcat -b localhost:9092 -L | grep -E 'orders|payments'
```

You should see both topic names listed. If they are missing, wait a few seconds and run the command again.

## Step 5: Cleanup

Stop the "Legacy" source cluster to save resources.

```bash
docker compose -f docker/docker-compose-lab3.yml down
```

## Next Steps

Proceed to [Lab 4: Managed Data Pipelines](lab-4-pipelines.md) to learn how to transform data in flight.
