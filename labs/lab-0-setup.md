# Lab 0: Setup

**Prerequisites**: Docker

## Learning Objectives

By the end of this lab, you will:

- Install the WarpStream CLI tool.
- Set up a local development environment (Playground).
- Verify connectivity to the WarpStream Virtual Cluster.

## Step 1: Install WarpStream CLI

The CLI tool is used to manage WarpStream resources and start the playground.

**Linux/macOS:**

```bash
curl https://console.warpstream.com/install.sh | bash
```

## Step 2: Install kcat

`kcat` (formerly kafkacat) is a generic command-line non-JVM producer and consumer for Kafka. We will use it throughout the workshop to interact with WarpStream.

**macOS:**

```bash
brew install kcat
```

**Linux:**

```bash
apt-get install kafkacat
```



## Step 3: Install jq

`jq` is a JSON formatter used by the configure / evolve scripts to parse Pipeline API responses.

**macOS:**

```bash
brew install jq
```

**Linux:**

```bash
apt-get install jq
```



## Step 4: Start WarpStream Playground

The playground starts a local WarpStream Agent, a simulated S3 bucket, and other helper services in Docker.

```bash
mkdir /tmp/warpstream-data
```

```bash
warpstream playground -bucketURL "file:///tmp/warpstream-data"
```

You should see output similar to:

```
Server will shutdown automatically after: 4h0m0s
Signing up for temporary account...
Creating Schema Registry cluster...
Creating Tableflow cluster...
Creating temporary data directory: 
Starting local agents...
```

**Important Configuration Steps:**

1. **Open the Console**: Copy the URL from the output and open it in your browser. This is the WarpStream Control Plane.
2. **Collect Credentials** (You will need these for later labs):
  - **VCI_ID**: Go to **Dashboard** → **Cluster** → Copy **Virtual Cluster ID**.
  - **API Key**: Go to **Dashboard** → **API Keys** → **Default Key**.

> **Note**: Keep this terminal window open. The playground will run for 4 hours.



## Step 5: Verify Connection

Open a **new terminal window** to run commands against the playground. Verify that `kcat` can reach the WarpStream Agent.

```bash
kcat -b localhost:9092 -L
```

**Expected Output:**

```
Metadata for all topics (from broker -1: localhost:9092/bootstrap):
 1 brokers:
  broker XXXXXXXXX at localhost:9092 (controller)
 0 topics:
```



## Step 6: Quick Test

Let's ensure we can write and read data.

**Produce a message:**

```bash
echo "hello warpstream" | kcat -b localhost:9092 -t test-topic -P
```

**Consume the message:**

```bash
kcat -b localhost:9092 -t test-topic -C -e
```

**Expected Output:**

```
hello warpstream
```



## Summary

You have successfully:

- Installed the necessary tools.
- Started a local WarpStream Virtual Cluster.
- Verified that it accepts standard Kafka commands.



## Next Steps

Proceed to [Lab 1: Producer & Consumer](lab-1-producer-consumer.md) to dive deeper into messaging.