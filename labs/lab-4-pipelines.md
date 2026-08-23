# Lab 4: Managed Data Pipelines

## Learning Objectives

By the end of this lab, you will:
1. Create managed pipelines via the WarpStream Console
2. Transform Kafka messages with key-value transformations (WarpStream to WarpStream)
3. Write processed data to local files with date partitioning

## Concept Introduction

WarpStream Managed Data Pipelines uses **Bento** embedded directly in WarpStream Agents. This eliminates the need for separate Kafka Connect clusters or stream processing infrastructure.

Pipelines are created and managed through:
- **WarpStream Console**: Web UI at https://console.warpstream.com
- **WarpStream API**: Programmatic access via REST API

### Architecture

```
Stage 1: Transformation
+------------------+     +--------------------+     +------------------------+
| Topic: raw-events| --> | Transform Pipeline | --> |Topic: transformed-events|
+------------------+     +--------------------+     +------------------------+
                        (Parse JSON, Add Fields, Drop PII)
                                    |
                                    v
Stage 2: Sink
                        +--------------------+     +------------------------+
                        | File Sink Pipeline | --> | Local Files            |
                        +--------------------+     +------------------------+
                        (Batching, Partitioning)   ./output/events/
```

---

## Part 1: Environment Setup

### Step 1: Create Kafka Topics

Create the required topics using kcat or kafka CLI:

```bash
# Using kcat (topics auto-create on first message)
echo '{"init": true}' | kcat -b localhost:9092 -t raw-events -P
echo '{"init": true}' | kcat -b localhost:9092 -t transformed-events -P
```

### Step 2: Verify Topics

```bash
kcat -b localhost:9092 -L | grep -E "topic|partition"
```

---

## Part 2: Create Transform Pipeline (Console)

This pipeline transforms messages from `raw-events` to `transformed-events`.

### Step 1: Open WarpStream Console

1. Go to https://console.warpstream.com
2. Navigate to your cluster (e.g., `vcn_default`)
3. Click the **Pipelines** tab
4. Click **+ Create Pipeline**

### Step 2: Name Your Pipeline

Enter a name: `transform-pipeline`

### Step 3: Paste Pipeline Configuration

Copy and paste this YAML into the pipeline editor:

```yaml
input:
  kafka_franz_warpstream:
    topics: ["raw-events"]
    consumer_group: "transform-pipeline-v3"

pipeline:
  processors:
    - mapping: |
        root = this
        root.processed_at = now()
        root.event_date = now().ts_format("2006-01-02")
        root.event_hour = now().ts_format("15")
        root.is_high_value = this.amount.or(0) > 100
        root.priority = if this.amount.or(0) > 500 {
          "high"
        } else if this.amount.or(0) > 100 {
          "medium"
        } else {
          "low"
        }
        root.customer_id = this.user_id
        root.transaction_amount = this.amount
        root = root.without("user_id", "amount", "internal_id", "ssn", "credit_card")

output:
  kafka_franz_warpstream:
    topic: "transformed-events"
    key: "${! json(\"customer_id\") }"

warpstream:
  cluster_concurrency_target: 1
```

### Step 4: Save and Enable

1. Click **Save** to save the configuration
2. Toggle **Paused** to enable the pipeline

### Key Transformations Explained

| Transformation | Bloblang Example |
|----------------|------------------|
| Add timestamp | `root.processed_at = now()` |
| Conditional logic | `root.priority = if this.amount > 500 { "high" } else { "low" }` |
| Rename field | `root.customer_id = this.user_id` |
| Drop PII | `root = this.without("ssn", "credit_card")` |

---

## Part 3: Create File Sink Pipeline (Console)

This pipeline reads from `transformed-events` and writes to local files with date partitioning.

### Step 1: Create Output Directory

First, create the output directory on the host where WarpStream agents run:

```bash
mkdir -p ./output/events
```

### Step 2: Create New Pipeline

1. In the Pipelines tab, click **+ Create Pipeline**
2. Name it: `file-sink-pipeline`

### Step 3: Paste Pipeline Configuration

Copy and paste this YAML:

```yaml
input:
  kafka_franz_warpstream:
    topics: ["transformed-events"]
    consumer_group: "file-sink-pipeline-v3"

pipeline:
  processors:
    # Skip bootstrap / bad messages that lack partition fields
    - mapping: |
        root = if this.event_date.or("") == "" {
          deleted()
        } else {
          this
        }
    - mapping: |
        root = this
        meta partition_path = "year=" + this.event_date.string().split("-").index(0) +
                             "/month=" + this.event_date.string().split("-").index(1) +
                             "/day=" + this.event_date.string().split("-").index(2) +
                             "/hour=" + this.event_hour.string()

buffer:
  memory:
    limit: 52428800
    batch_policy:
      enabled: true
      count: 10
      byte_size: 1048576
      period: "10s"

output:
  file:
    path: "./output/events/${! metadata(\"partition_path\") }/${! uuid_v4() }.json"
    codec: lines

warpstream:
  cluster_concurrency_target: 1
```

**File Output Options** (from [Bento docs](https://warpstreamlabs.github.io/bento/docs/components/outputs/file/)):

| Codec | Description |
|-------|-------------|
| `lines` | Append each message followed by a newline |
| `all-bytes` | Write full message, overwrite existing file |
| `append` | Append without delimiter |
| `delim:x` | Append with custom delimiter (e.g., `delim:\t`) |

### Step 4: Save and Enable

1. Click **Save**
2. Toggle **Paused** to enable

### Step 5: Spot-check transform output, then verify files

After both pipelines are enabled, produce a few sample events (Part 5 below), then confirm the transform pipeline wrote enriched records to `transformed-events`:

```bash
kcat -b localhost:9092 -t transformed-events -C -o -5 -e
```

`-o -5` reads the last 5 messages so you are looking at fresh output, not the earlier `{"init": true}` bootstrap. You should see fields like `priority`, `event_date`, and `customer_id`. Sensitive fields such as `ssn` / `credit_card` / `user_id` should be gone.

Then check that the file sink wrote partitioned JSON under `./output/events`:

```bash
# List generated files
find ./output/events -type f -name '*.json'

# View file contents
cat ./output/events/year=*/month=*/day=*/hour=*/*.json | jq .

# Count messages written
cat ./output/events/year=*/month=*/day=*/hour=*/*.json | wc -l
```

---

## Part 4: Create Pipelines via API (Alternative)

You can also create pipelines programmatically using the WarpStream API.

### Step 1: Create Pipeline

```bash
curl https://api.warpstream.com/api/v1/create_pipeline \
  -H 'warpstream-api-key: YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "virtual_cluster_id": "vci_xxx",
    "pipeline_name": "transform-pipeline"
  }'
```

Response:
```json
{
  "pipeline_id": "pipeline-xyz",
  "pipeline_name": "transform-pipeline",
  "pipeline_state": "paused",
  "pipeline_type": "bento"
}
```

### Step 2: Add Pipeline Configuration

```bash
curl https://api.warpstream.com/api/v1/create_pipeline_configuration \
  -H 'warpstream-api-key: YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "virtual_cluster_id": "vci_xxx",
    "pipeline_id": "pipeline-xyz",
    "configuration_yaml": "input:\n  kafka_franz_warpstream:\n    topics: [\"raw-events\"]\n    consumer_group: \"transform-pipeline-v3\"\n\npipeline:\n  processors:\n    - mapping: |\n        root = this\n        root.processed_at = now()\n        root.event_date = now().ts_format(\"2006-01-02\")\n        root.event_hour = now().ts_format(\"15\")\n        root.is_high_value = this.amount.or(0) > 100\n        root.priority = if this.amount.or(0) > 500 {\n          \"high\"\n        } else if this.amount.or(0) > 100 {\n          \"medium\"\n        } else {\n          \"low\"\n        }\n        root.customer_id = this.user_id\n        root.transaction_amount = this.amount\n        root = root.without(\"user_id\", \"amount\", \"internal_id\", \"ssn\", \"credit_card\")\n\noutput:\n  kafka_franz_warpstream:\n    topic: \"transformed-events\"\n    key: \"${! json(\\\"customer_id\\\") }\"\n\nwarpstream:\n  cluster_concurrency_target: 1"
  }'
```

### Step 3: Enable Pipeline

```bash
curl https://api.warpstream.com/api/v1/change_pipeline_state \
  -H 'warpstream-api-key: YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "virtual_cluster_id": "vci_xxx",
    "pipeline_id": "pipeline-xyz",
    "desired_state": "running"
  }'
```

---

## Part 5: Test the Pipelines

### Produce Sample Events

```bash
cat <<EOF | kcat -b localhost:9092 -t raw-events -P
{"user_id": "user123", "amount": 150.50, "event_type": "purchase", "internal_id": "xyz", "ssn": "123-45-6789"}
{"user_id": "user456", "amount": 75.00, "event_type": "purchase", "internal_id": "abc"}
{"user_id": "user789", "amount": 600.00, "event_type": "purchase", "credit_card": "1234-5678-9012-3456"}
{"user_id": "test-user", "amount": 25.00, "event_type": "test"}
EOF
```

### Generate Batch Test Data

```bash
for i in {1..20}; do
    AMOUNT=$((RANDOM % 1000))
    USER_ID="user$(printf %04d $i)"
    echo "{\"user_id\": \"$USER_ID\", \"amount\": $AMOUNT, \"event_type\": \"purchase\"}"
done | kcat -b localhost:9092 -t raw-events -P
```

### Consume Transformed Events

```bash
kcat -b localhost:9092 -t transformed-events -C -o -20 -c 20 | jq .
```

This pulls the newest ~20 messages so you are inspecting recent transforms rather than bootstrap/`init` records from the start of the topic.

### Inspect Raw Events

You can also peek at the input topic directly:

```bash
kcat -b localhost:9092 -t raw-events -C -o -10 -c 10 | jq .
```

---

## Expected Transformation

**Input (raw-events):**
```json
{
  "user_id": "user123",
  "amount": 150.50,
  "event_type": "purchase",
  "internal_id": "internal-xyz",
  "ssn": "123-45-6789"
}
```

**Output (transformed-events):**
```json
{
  "customer_id": "user123",
  "transaction_amount": 150.50,
  "event_type": "purchase",
  "processed_at": "2026-01-28T10:30:45Z",
  "event_date": "2026-01-28",
  "event_hour": "10",
  "is_high_value": true,
  "priority": "medium"
}
```

**Note:** Sensitive fields (`internal_id`, `ssn`, `credit_card`) are removed.

---

## Next Steps

Proceed to [Lab 5: Schema Registry](lab-5-schema-registry.md) to learn how to enforce data quality.
