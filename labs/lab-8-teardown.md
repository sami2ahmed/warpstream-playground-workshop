# Lab 8: Teardown

**Prerequisites**: You have run one or more of Labs 0–7

## Learning Objectives

Simply teardown steps for the labs run prior. Note that there's not really anything that has been created that will incur costs, it's more to free up memory etc. on your computer. 

## Concept Introduction

The workshop leaves behind five kinds of state, and only the first disappears on its own:


| What                                                   | Where                                  | Survives a reboot?                           |
| ------------------------------------------------------ | -------------------------------------- | -------------------------------------------- |
| Virtual cluster (Kafka, Schema Registry, Tableflow)    | WarpStream cloud, ephemeral            | No — expires with the playground             |
| Containers, volumes, networks                          | Docker                                 | Yes                                          |
| Bucket and Iceberg data                                | `/tmp/warpstream-*`                    | Usually not (`/tmp` is cleared periodically) |
| Rendered Prometheus config **containing your app key** | `docker/prometheus/prometheus.yml.tmp` | Yes                                          |
| Local credentials                                      | `.env`                                 | Yes                                          |


---

## Step 1: Stop the Playground

The playground shuts down on its own after its timeout (4h by default), but you can stop it explicitly so ports 9092, 9094, and 8080 are released.

Press `Ctrl+C` in the terminal running `warpstream playground`. To confirm:

```bash
pgrep -fl "warpstream playground" || echo "playground stopped"
```

> **Note**: When the playground exits, its virtual cluster, all topics, all consumer group
> offsets, and all registered schemas are destroyed. The `VCI_ID` and app key in your `.env` become dead immediately. Nothing you produced during Labs 1–7 is recoverable, so export anything you want to keep *before* this step.



## Step 2: Stop the Docker Stacks

Tear the stacks down in reverse lab order. Lab 7 needs `-v` to clear some grafana data that could get left behind and mess up the `admin` / `admin` re-logins.  

```bash
docker-compose -f docker/docker-compose-lab7.yml down -v
docker-compose -f docker/docker-compose-lab6.yml down
docker-compose -f docker/docker-compose-lab3.yml down
```



## Step 3: Remove the Rendered Credentials

**Do this even if you skip the rest of the cleanup.** Lab 7 renders `prometheus.yml` into
`prometheus.yml.tmp` with your app key substituted in plaintext. That file is untracked, and the
repository has no `.gitignore`, so a `git add -A` would commit it.

```bash
grep -c "aks_" docker/prometheus/prometheus.yml.tmp   # confirm it holds a key
rm -rf docker/prometheus/prometheus.yml.tmp
```

Use `rm -rf`, not `rm`: if you ever ran Lab 7 before rendering the template, Docker created
`prometheus.yml.tmp` as a *directory*.

While you are here, protect the credentials file itself:

```bash
printf '.env\n' >> .gitignore
```



## Step 4: Remove Workshop Data

The playground bucket is the bulk of it (~39 MB after a full run):

```bash
rm -rf /tmp/warpstream-data
rm -rf ./output/events        # Lab 4 file sink, if you ran it
```



### Decide deliberately about the Iceberg data

```bash
# Inspect before deleting
du -sh /tmp/warpstream-tableflow-iceberg
ls /tmp/warpstream-tableflow-iceberg/warpstream/_tableflow/*/metadata/
```

Iceberg tables are only produced once Tableflow has been configured through the WarpStream Console, so an existing table is the one artifact you cannot regenerate from the command line. Keeping it lets you re-run the queries in Lab 6 without reconfiguring Tableflow. Delete it only when you are finished with Lab 6 for good:

```bash
rm -rf /tmp/warpstream-tableflow-iceberg
```



## Step 5: Reclaim Docker Disk Space (Optional)

A full workshop run pulls roughly 6 GB of images. Check first:

```bash
docker system df
```

Remove only the workshop images, by name, so you do not disturb unrelated projects:

```bash
docker rmi docker-duckdb:latest \
           confluentinc/cp-kafka:7.6.0 \
           confluentinc/cp-zookeeper:7.6.0 \
           prom/prometheus:latest \
           grafana/grafana:latest
```

The `docker-duckdb` image is built locally by Lab 6, so removing it means a rebuild next time.

## Step 6: (Optional) Verification Steps

Everything below should report nothing:

```bash
# No workshop containers
docker ps -a --format '{{.Names}}' | grep -E "lab3|lab6|warpstream" || echo "no containers"

# No workshop volumes or networks
docker volume ls --format '{{.Name}}'  | grep -E "docker_grafana|docker_prometheus" || echo "no volumes"
docker network ls --format '{{.Name}}' | grep -E "docker_" || echo "no networks"

# No listeners on any workshop port
#   9092 Kafka | 9093 Lab 3 Kafka | 9094 Schema Registry | 9090 Prometheus | 3001 Grafana
lsof -nP -iTCP:9092,9093,9094,9090,3001 -sTCP:LISTEN || echo "all ports free"

# No playground process
pgrep -fl "warpstream playground" || echo "playground stopped"
```

Finally, check that no credentials are staged for commit:

```bash
git status --short
```

`.env` may appear as untracked, which is fine. `docker/prometheus/prometheus.yml.tmp` should not
appear at all.

## Summary

You have:

- Stopped the playground and released ports 9092, 9094, and 8080.
- Removed all containers, volumes, and networks from Labs 3, 6, and 7 without disturbing the
other stacks.
- Deleted the rendered Prometheus config.
- Removed the playground bucket optionally along with the iceberg data.
- Verified that nothing is left listening or running.

## Some notes on rerunning these labs again 

The playground issues a **new** virtual cluster every time, so credentials never carry over:

1. `mkdir -p /tmp/warpstream-data` (use `-p`; plain `mkdir` fails if the directory exists)
2. `warpstream playground -bucketURL "file:///tmp/warpstream-data"`
3. Update `VCI_ID` in `.env` with the new `vci_...` from the startup output.
4. Create a fresh app key in the Console and update `WARPSTREAM_APP_KEY`.
5. Re-render `prometheus.yml.tmp` before starting Lab 7 again (should be handled when you run the `run-lab-7.sh`

