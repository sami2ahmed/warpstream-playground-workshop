# Lab 6: Tableflow - Kafka to Iceberg

## Learning Objectives

By the end of this lab, you will:

- Understand how WarpStream Tableflow automates the Streaming-to-Lakehouse path.
- Configure a Tableflow pipeline to write Apache Iceberg tables.
- Query streaming data using DuckDB and PyIceberg.
- Inspect Iceberg metadata files (snapshots and manifests).

## Concept Introduction

**Tableflow** is a managed service within the WarpStream Agent that continuously reads from Kafka topics and writes **Apache Iceberg** tables to object storage.

### Architecture

```
WarpStream
+----------------------+       +-----------------+
| Kafka Topic: orders  | ----> | Tableflow Agent |
+----------------------+       +-----------------+
                                       |
                                       | (Writes)
                                       v
Object Storage            +-------------------------+
                          |      Parquet Files      |
                          +-------------------------+
                                       ^
                          +-------------------------+
                          |    Iceberg Metadata     |
                          +-------------------------+
                                       |
Analytics                              | (Reads)
                          +-------------------------+
                          |         DuckDB          |
                          +-------------------------+
```

---



## Part 1: Environment Setup



## Step 1: Create Local Storage

In a real deployment, this would be an S3 bucket. Locally, we use a directory.

```bash
mkdir -p /tmp/warpstream-tableflow-iceberg
```



## Step 2: Start DuckDB

We need a query engine that understands Iceberg.

```bash
docker compose -f docker/docker-compose-lab6.yml up -d duckdb
```



## Step 3: Produce Data

Let's generate "Order" events to the `ecommerce-orders` topic.

```bash
chmod +x scripts/produce-sample-orders.sh
scripts/produce-sample-orders.sh ecommerce-orders 50
```

---



## Part 2: Configure Tableflow



## Step 1: Create Configuration

Define the mapping from Kafka Topic to Iceberg Table.

Go to the **WarpStream Console** --> **Tableflow** (left hand menu) --> **Create**  

Then simply copy paste the yaml below, save it, and toggle the radio button 

```yaml
# /tmp/tableflow-config.yaml
source_clusters:
  - name: "playground_kafka"
    bootstrap_brokers:
      - hostname: "localhost"
        port: 9092

tables:
  - source_cluster_name: "playground_kafka"
    source_topic: "ecommerce-orders"
    source_format: "json"
    schema_mode: "inline"
    destination_bucket_url: "file:///tmp/warpstream-tableflow-iceberg"
    input_schema: |
      {
        "type": "object",
        "properties": {
          "order_id": {"type": "string"},
          "customer_id": {"type": "string"},
          "timestamp": {"type": "string", "format": "date-time"},
          "total_amount": {"type": "number"},
          "status": {"type": "string"},
          "payment_method": {"type": "string"}
        },
        "required": ["order_id", "total_amount", "status"]
      }
```



## Step 2: Apply Configuration

*For this workshop, the agent automatically picks up the configuration if linked to the correct Virtual Cluster ID.*

---



## Part 3: Verify and Query



## Step 1: Inspect Files

Wait about 30-60 seconds for the first batch to be written.

```bash
ls -R /tmp/warpstream-tableflow-iceberg/
```

You should see `metadata/` and `data/` directories.

## Step 2: Query with DuckDB

We will use a Python script (`pyiceberg`) to read the metadata and pass the file list to DuckDB for querying.

```bash
# Run the provided query script inside the Docker container
docker exec -i lab6-duckdb python3 /data/scripts/query-iceberg-with-pyiceberg.py
```

**Expected Output:**

- Total Orders count.
- Revenue aggregation by status.

