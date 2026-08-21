# Lab 5: Schema Registry

**Duration**: 30 minutes
**Difficulty**: Intermediate
**Prerequisites**: Lab 4 completed

## Learning Objectives

By the end of this lab, you will:
- Understand how Schema Registry prevents "bad data".
- Register Avro and Protobuf schemas via REST API.
- Evolve schemas safely using compatibility checks.

## Concept Introduction

In a decoupled architecture, Producers and Consumers often don't talk to each other. **Schema Registry** acts as the contract store, ensuring that the data shape (Schema) is agreed upon.

### Architecture

```
+----------+      1. Register/Get ID      +-----------------+
| Producer | ---------------------------> | Schema Registry |
+----------+ <--------------------------- +-----------------+
     |              (Returns ID: 1)                ^
     |                                             |
     | 2. Produce (Message + Schema ID)            | 3. Get Schema (ID: 1)
     v                                             |
+----------+                               +----------+
|  Topic   | ----------------------------> | Consumer |
+----------+                               +----------+
```

---

## Part 1: Schema Registry Basics

## Step 1: Verify Environment

The WarpStream playground includes a Schema Registry compatible with standard clients.

**Check if it's running:**
```bash
curl -s http://localhost:9094/subjects | jq .
```
*Expected output: `[]` (Empty array).*

## Step 2: Register a Schema (Avro)

We will define a `User` schema. In Schema Registry, schemas are grouped by **Subject**.
Convention: `topic-name-value`.

**Subject**: `users-value`
**Schema**:
```json
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}
```

**Register it via API:**
```bash
curl -X POST http://localhost:9094/subjects/users-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}"}'
```

*Expected output: `{"id":1}`*

## Step 3: Retrieve Schema

You can fetch schemas by Subject or globally by ID.

**Get latest version:**
```bash
curl -s http://localhost:9094/subjects/users-value/versions/latest | jq .
```

---

## Part 2: Schema Evolution

Business requirements change. We need to add an `email` field to our User.

## Step 1: Check Compatibility Mode

Schema Registry enforces rules to ensure changes don't break consumers.

```bash
# Get current compatibility config (Default is usually BACKWARD)
curl -s http://localhost:9094/config | jq .
```

**Set to BACKWARD:**
```bash
curl -X PUT http://localhost:9094/config/users-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'
```

> **BACKWARD Compatibility**: New schema can read old data. (Consumer upgrades first).

## Step 2: Evolve Schema (Valid)

Add an optional `email` field. This is **backward compatible** because old data (without email) is still valid (email defaults to null).

```bash
curl -X POST http://localhost:9094/subjects/users-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}'
```

*Expected output: `{"id":2}`*

## Step 3: Attempt Invalid Evolution

Try to change the `age` field from `int` to `string`. This is a **type mismatch** that breaks compatibility.

```bash
curl -X POST http://localhost:9094/subjects/users-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"string\"}]}"}'
```

*Expected error (40901 Conflict):*
```json
{"error_code":40901,"message":"schema being registered is incompatible with an earlier schema for subject 'users-value', details: [{errorType:TYPE_MISMATCH, description:reader schema string not compatible with writer schema int}]"}
```

**Check compatibility before registering:**
```bash
curl -s -X POST http://localhost:9094/compatibility/subjects/users-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"string\"}]}"}'
```

*Expected output:* `{"is_compatible":false}`

> **Key Insight**: Schema Registry acts as a gatekeeper, preventing producers from publishing data that would break existing consumers.

---

## Part 3: Using kcat with Schema Registry

Now let's use `kcat` to work with messages and Schema Registry.

### Step 1: Produce JSON Messages

Produce JSON messages that conform to our registered schema:

```bash
echo '{"name": "Alice", "age": 30}' | kcat -b localhost:9092 -t users-avro -P
echo '{"name": "Bob", "age": 25}' | kcat -b localhost:9092 -t users-avro -P
echo '{"name": "Charlie", "age": 35}' | kcat -b localhost:9092 -t users-avro -P
```

**Produce multiple messages at once:**
```bash
cat <<EOF | kcat -b localhost:9092 -t users-avro -P
{"name": "Diana", "age": 28}
{"name": "Eve", "age": 32}
EOF
```

### Step 2: Consume Messages with kcat

**Basic consumption:**
```bash
kcat -b localhost:9092 -t users-avro -C -e
```

**Consume with metadata (JSON output):**
```bash
kcat -b localhost:9092 -t users-avro -C -J -e | jq -c '.'
```

*Expected output:*
```json
{"topic":"users-avro","partition":0,"offset":0,"tstype":"create","ts":1234567890,"key":null,"payload":"{\"name\": \"Alice\", \"age\": 30}"}
{"topic":"users-avro","partition":0,"offset":1,"tstype":"create","ts":1234567891,"key":null,"payload":"{\"name\": \"Bob\", \"age\": 25}"}
```

**Formatted output with partition and offset:**
```bash
kcat -b localhost:9092 -t users-avro -C \
  -f 'Partition: %p | Offset: %o | Value: %s\n' -e
```

### Step 3: Consumer Schema Validation with kcat

The `-s avro -r <url>` flags enable kcat to deserialize Avro-encoded messages using Schema Registry. This acts as a **consumer-side validation** - messages that don't conform to the expected Avro format are rejected.

**Test: Try to consume plain JSON as Avro (will fail):**
```bash
# First, produce plain JSON (not Avro-encoded)
echo '{"name": "TestUser", "age": 30}' | kcat -b localhost:9092 -t schema-test -P

# Try to consume with Avro deserializer - FAILS!
kcat -b localhost:9092 -t schema-test \
  -C \
  -r http://localhost:9094 \
  -s value=avro \
  -e
```

*Expected error:*
```
% ERROR: Failed to format message in schema-test [0] at offset 0: 
Avro/Schema-registry message deserialization: Invalid CP1 magic byte 123, 
expected 0: message not produced with Schema-Registry Avro framing: terminating
```

> **Key Insight**: The magic byte `123` is `{` (start of JSON). Avro-encoded messages must start with magic byte `0` followed by a 4-byte schema ID. kcat rejects messages that don't have proper Avro framing.

**When it works**: If messages were produced by an Avro-aware producer (like `kafka-avro-console-producer` or a Java/Python client with Schema Registry), kcat will successfully deserialize them:

```bash
kcat -b localhost:9092 -t properly-encoded-avro-topic \
  -C \
  -r http://localhost:9094 \
  -s value=avro \
  -e
```

> **Note**: kcat's `-s avro` is **consumer-only**. To produce Avro-encoded messages, use Confluent's `kafka-avro-console-producer` or a programmatic client (Java, Python, Go) with Schema Registry integration.

### Step 4: Validate Schema Conformance

You can validate that your JSON conforms to the schema using curl:

```bash
# Check if a schema is compatible
curl -X POST http://localhost:9094/compatibility/subjects/users-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}"}'
```

*Expected output:* `{"is_compatible":true}`

### Step 5: List Topics and Verify

```bash
# List all topics
kcat -b localhost:9092 -L | grep "topic"

# Check registered schemas
curl -s http://localhost:9094/subjects | jq .
```

### kcat Reference for Schema Registry (Consumer Only)

| Option | Description |
|--------|-------------|
| `-r <url>` | Schema Registry URL (required with `-s avro`) |
| `-s value=avro` | Deserialize Avro-encoded message values (consumer only) |
| `-s key=avro` | Deserialize Avro-encoded message keys (consumer only) |
| `-J` | Output messages as JSON with metadata |
| `-f <format>` | Custom output format (`%p`=partition, `%o`=offset, `%s`=payload) |

**Avro Message Format** (what kcat expects when using `-s avro`):
```
[0x00][4-byte schema ID][Avro binary payload]
  ^         ^                   ^
  |         |                   └── Actual data
  |         └── Schema ID from registry
  └── Magic byte (must be 0)
```

> **Important**: kcat's `-s` flag is **consumer-only** (deserialization). Attempting to use it with `-P` (producer) will fail with: `ERROR: -s serdes only available in the consumer`

---

## Summary

-   **Schema Registry** is the "single source of truth" for data structures.
-   **Evolution** allows schemas to change over time without breaking applications.
-   **Compatibility Modes** (BACKWARD, FORWARD, FULL) dictate the rules for evolution.
-   **kcat** can consume Avro-encoded messages using `-s avro -r <registry-url>` and provides rich JSON output with `-J`.

## Next Steps

Proceed to [Lab 6: Tableflow](lab-6-tableflow.md) to see how schemas enable automatic data warehousing.
