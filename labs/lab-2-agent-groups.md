# Lab 2: WarpStream Agent Groups

## Learning Objectives

By the end of this lab, you will:
- Understand the concept of **Agent Groups** in WarpStream.
- Run multiple agents with different group configurations locally.
- Connect specifically to an Agent Group.
- Inspect cluster metadata to verify isolation.

## Concept Introduction

In a single WarpStream Virtual Cluster, you can have multiple "groups" of Agents. This allows you to route traffic to specific sets of agents based on workload requirements (e.g., isolating high-throughput producers from sensitive consumers, or geographic routing).

### Architecture

```
     +--------------------+             +--------------------+
     | Client A (Default) |             | Client B (Group-1) |
     +--------------------+             +--------------------+
               |                                   |
               v                                   v
+-----------------------------------------------------------------+
| Virtual Cluster                                                 |
|                                                                 |
|  +-----------------------+           +-----------------------+  |
|  | Agent Group: default  |           | Agent Group: group-1  |  |
|  | [Agent 1 :9092]       |           | [Agent 2 :9096]       |  |
|  +-----------------------+           +-----------------------+  |
|               |                                   |             |
+---------------|-----------------------------------|-------------+
                |                                   |
                v                                   v
         +------------------------------------------------+
         |                 Object Storage                 |
         +------------------------------------------------+
```

-   **Default Group**: Standard entry point.
-   **Agent Group**: Isolated set of agents (e.g., `group-1`).

---
## Step 1: Start a Second Agent (Agent Group)

We will simulate a second group of agents running on a different port (`9096`).

1.  **Open the Console**.

2.  **Retrieve Credentials**:
    -   Go to the WarpStream Console URL provided in Step 1.
    -   **Agent Key**: `Dashboard` -> `Virtual Cluster` -> `Agent Keys`
    -   **Virtual Cluster ID**: `Dashboard` -> `Virtual Cluster` -> `ID`.

    *For the playground, the default Virtual Cluster ID is usually fixed or visible in the startup logs.*

3.  **Run the Second Agent**:
    Replace `<YOUR_AGENT_KEY>` and `<YOUR_VCI>` with your actual values.

    ```bash
    warpstream agent \
        -agentKey <YOUR_AGENT_KEY> \
        -defaultVirtualClusterID <YOUR_VCI> \
        -bucketURL "file:///tmp/warpstream-data" \
        -metadataURL "https://prod-q.us-east1.gcp.warpstream.com" \
        -kafkaPort 9096 \
        -httpPort 8085 \
        -agentGroup group-1
    ```

    *   `-agentGroup group-1`: Assigns this agent to a specific group.
    *   `-kafkaPort 9096`: Listens on a different port to avoid conflict.
    *   `-bucketURL ...`: Points to the SAME data location as the playground.
    *   `-metadataURL ...`: Points the agent at the GCP control plane used by the playground. Using `-region` alone can fail against this environment.

    Keep this agent running in its own terminal. Open a second terminal for the `kcat` commands below.

    On macOS, you may see a system dialog asking whether to allow `warpstream` to accept incoming network connections. Approve it so clients can reach port `9096`.

## Step 3: Verify Connectivity

Now we have two "doors" into the same WarpStream cluster.

### Connect to Default Group
```bash
kcat -b localhost:9092 -L
```
*Expected Output*: You should see the broker on port 9092.

### Connect to Agent Group "group-1"
```bash
kcat -b localhost:9096 -L
```
*Expected Output*: You should see the broker on port 9096.

## Step 4: Metadata Inspection

Let's see how the cluster metadata differs depending on which agent you ask.

1.  **Ask the Default Agent**:
    ```bash
    kcat -b localhost:9092 -L | grep "broker"
    ```
    *Result*: Shows the default agent(s).

2.  **Ask the "group-1" Agent**:
    ```bash
    kcat -b localhost:9096 -L | grep "broker"
    ```
    *Result*: Shows the `group-1` agent(s).

This proves that clients connecting to `localhost:9096` are **pinned** to the agents in `group-1`, achieving isolation.

## Next Steps

Proceed to [Lab 3: WarpStream Orbit](lab-3-orbit.md) to learn about cluster linking and migrations.
