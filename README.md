# KafkaSortPipeline

**50-Million-Record Kafka Generation & External-Sort Pipeline**

A self-contained Docker pipeline that generates 50 million CSV records into Apache Kafka, sorts them three times (by ID, Name, and Continent) using an external merge-sort algorithm, and verifies correctness — all within a single 2 GB container running Kafka in KRaft mode (no ZooKeeper).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Docker Container (2 GB RAM, 4 CPUs)          │
│                                                                 │
│  ┌──────────────┐     ┌────────────────────────────────────┐   │
│  │ Kafka Broker  │     │          Java Applications         │   │
│  │  (KRaft mode) │     │                                    │   │
│  │  -Xmx384m     │     │  ┌────────────┐                   │   │
│  │               │◄────┤  │ Generator  │  -Xmx256m         │   │
│  │  Topics:      │     │  │ 50M records│                    │   │
│  │  ┌──────────┐ │     │  └─────┬──────┘                   │   │
│  │  │ source   │ │     │        │                           │   │
│  │  │ (4 part) │ │     │        ▼                           │   │
│  │  ├──────────┤ │     │  ┌────────────┐  ┌─────────────┐  │   │
│  │  │ id       │ │◄────┤  │   Sorter   │  │  Temp Disk  │  │   │
│  │  │ (1 part) │ │     │  │ -Xmx512m   │◄►│  Sorted     │  │   │
│  │  ├──────────┤ │     │  │            │  │  Runs        │  │   │
│  │  │ name     │ │     │  │ ID → NAME  │  └─────────────┘  │   │
│  │  │ (1 part) │ │     │  │  → CONTNT  │                   │   │
│  │  ├──────────┤ │     │  └─────┬──────┘                   │   │
│  │  │continent │ │     │        │                           │   │
│  │  │ (1 part) │ │     │        ▼                           │   │
│  │  └──────────┘ │     │  ┌────────────┐                   │   │
│  │               │────►│  │  Verifier  │  -Xmx128m         │   │
│  │               │     │  │ count+sort │                    │   │
│  │               │     │  │ +checksum  │                    │   │
│  └──────────────┘     │  └────────────┘                   │   │
│                        └────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Memory Budget

| Component      | Heap (`-Xmx`) | Purpose                          |
|----------------|---------------|----------------------------------|
| Kafka Broker   | 384 MB        | KRaft broker + controller        |
| Data Generator | 256 MB        | Produce 50M records to `source`  |
| External Sort  | 512 MB        | Buffer, sort, spill, merge       |
| Verifier       | 128 MB        | Count, sortedness & checksum     |
| **Container**  | **2 GB**      | Hard ceiling via `--memory=2g`   |

> [!NOTE]
> Components run sequentially (except the always-on broker), so heap allocations
> do not overlap. Peak memory ≈ Kafka (384 MB) + Sorter (512 MB) + OS/buffers.

---

## Quick Start

### Prerequisites

- Docker Engine ≥ 20.10 (with BuildKit recommended)
- ~3 GB free disk space for the image + temporary sort runs
- No ports need to be exposed (everything runs inside the container)

### Build & Run

```bash
# 1. Build the Docker image
./docker/scripts/build.sh

# 2. Run the full pipeline (50M records, 2 GB RAM, 4 CPUs)
./docker/scripts/run.sh

# 3. Run with fewer records for testing
./docker/scripts/run.sh 100000
```

On Windows (PowerShell):

```powershell
# Build
docker build -t kafkasortpipeline:latest -f docker/Dockerfile .

# Run (50M records)
docker run --rm --memory=2g --cpus=4 kafkasortpipeline:latest

# Run (100K records for quick test)
docker run --rm --memory=2g --cpus=4 -e RECORD_COUNT=100000 kafkasortpipeline:latest
```

### Expected Output

The pipeline prints progress for each phase and concludes with a timing report:

```
=== Pipeline Run Report ===
Phase                               Wall Time
-----------------------------------------
Kafka startup                       26.5s
Topic creation                      28.6s
Data generation (50000000)          597.2s
Sort by id                          1013.6s
Sort by name                        891.3s
Sort by continent                   853.7s
Verification                        786.3s
-----------------------------------------
TOTAL WALL CLOCK                    4197.6s
```

*(Measured on a constrained Docker container: `--memory=2g --cpus=4`. Actual
values depend on host hardware, disk speed, and container runtime.)*

---

## How It Works

### Data Generation

`DataGeneratorApp` produces `RECORD_COUNT` CSV records to the `source` topic
(4 partitions). Each record has the format:

```
<id>,<name>,<address>,<continent>
```

- **id**: randomly generated 32-bit signed integer (spanning positive and negative range)
- **name**: randomly generated string
- **address**: randomly generated string
- **continent**: one of `Africa`, `Asia`, `Australia`, `Europe`, `North America`, `South America`

The generator uses batched Kafka production with zstd compression to maximise
throughput.

### External Merge Sort

`ExternalSortApp` implements a **two-phase external merge sort**:

**Phase A — Sort & Spill**
1. Consume records from the `source` topic into an in-memory buffer.
2. When the buffer is full (bounded by `-Xmx512m`), sort it by the requested key.
3. Write the sorted buffer to disk as a *sorted run* file.
4. Repeat until all records are consumed.

**Phase B — K-Way Merge**
1. Open all sorted run files simultaneously.
2. Maintain a `PriorityQueue` (min-heap) with one entry per run file, keyed on the sort field.
3. Repeatedly poll the smallest element, produce it to the output topic, and refill from the same run file.
4. This produces a globally sorted stream with O(N log K) comparisons, where K = number of runs.

> [!IMPORTANT]
> Output topics (`id`, `name`, `continent`) each have **exactly 1 partition** to
> guarantee global ordering. Multi-partition output would only guarantee
> per-partition ordering.

### Sequential Execution

The three sorts (ID → NAME → CONTINENT) run **sequentially**, not in parallel.
This is a deliberate design choice: running them concurrently would require
3 × 512 MB = 1.5 GB of sort heap alone, blowing the 2 GB container budget.

---

## Verification

`VerifyApp` performs three correctness checks on each output topic:

| Check              | Description                                                  |
|--------------------|--------------------------------------------------------------|
| **Count**          | Output topic has exactly the same number of records as input |
| **Sortedness**     | Every consecutive pair satisfies `a[i] ≤ a[i+1]`            |
| **Content Integrity** | 64-bit checksum (sum of hash codes) matches the source   |

### Running Verification Standalone

If you have a running container:

```bash
docker exec <container_id> /app/scripts/verify.sh
```

---

## Kafka Tuning & Optimizations

### KRaft Mode (No ZooKeeper)

Running Kafka in KRaft mode eliminates the need for a separate ZooKeeper JVM,
saving **~300–400 MB** of heap that would otherwise be consumed by ZooKeeper.
This is critical within the 2 GB memory budget.

### Producer Tuning

| Setting              | Value    | Rationale                                              |
|----------------------|----------|--------------------------------------------------------|
| `acks`               | `1`     | Single broker; `acks=all` is equivalent but slower     |
| `linger.ms`          | `15`    | Allows micro-batching for higher throughput             |
| `batch.size`         | `131072` | 128 KB batches reduce request overhead                 |
| `compression.type`   | `zstd`  | Best compression ratio for CSV text data               |
| `buffer.memory`      | `33554432` | 32 MB cap to stay within heap budget                |

### Consumer Tuning

| Setting                    | Value    | Rationale                                         |
|----------------------------|----------|---------------------------------------------------|
| `fetch.min.bytes`          | `65536`  | 64 KB min fetch reduces round-trips               |
| `max.partition.fetch.bytes`| `1048576`| 1 MB per partition for large batch reads           |
| `enable.auto.commit`       | `false`  | Manual commit for exactly-once sort semantics      |

### Broker Tuning

| Setting                | Value     | Rationale                                          |
|------------------------|-----------|----------------------------------------------------|
| `KAFKA_HEAP_OPTS`      | `-Xmx384m` | Minimal heap for single-node broker              |
| `log.segment.bytes`    | `64 MB`  | Smaller segments = faster cleanup                   |
| `log.retention.hours`  | `1`      | Aggressive retention; data is consumed immediately  |
| GC                     | G1GC     | Predictable pause times under constrained heap      |
| `log.cleaner.threads`  | `1`      | Minimal cleaner threads for single-node setup       |

### Topic Design

- **`source` (4 partitions)**: Parallelism for production; consumer reads all partitions.
- **`id`, `name`, `continent` (1 partition each)**: **Single partition guarantees global
  ordering**, which is a hard requirement for sort output correctness. Multi-partition
  output would only provide per-partition ordering.

---

## Bottleneck Analysis

> [!NOTE]
> All numbers below are from the final 50M-record run under strict resource
> constraints (`--memory=2g`, `--cpus=4`). Peak container RSS stayed at
> **1.37 GiB / 2 GiB (68.5%)**, with `OOMKilled: false`.

### Measured Timing Breakdown

| Phase | Wall Time | Notes |
|---|---:|---|
| Kafka startup | 26.5s | KRaft controller + broker election |
| Topic creation | 28.6s | 4 topics (source ×4 partitions, 3 output ×1) |
| Data generation | 597.2s | 50M records → `source` (4 partitions, zstd) |
| Sort by id | 1013.6s | 39 runs → merge → `id` topic |
| Sort by name | 891.3s | 39 runs → merge → `name` topic |
| Sort by continent | 853.7s | 39 runs → merge → `continent` topic |
| Verification | 786.3s | 7 sequential consumer scans (4 count + 3 sort) |
| **TOTAL** | **4197.6s** | **~70 minutes** |

### 1. Why id-sort Is Slower Than name/continent Sort

All three sorts produced **identical run counts (39 runs each)**, so fan-in and
merge-tree depth are identical. The ~15% overhead on `id` is attributable to
**cold-start costs** that the first sort phase absorbs:

- **JIT warmup**: The HotSpot C2 compiler hasn't yet compiled the sort/merge
  hot paths when `sort-id` begins. By the time `sort-name` runs, the critical
  methods (`Record.fromCSV`, `RunWriter.spillRun`, `RunMerger.merge`) are fully
  JIT-compiled.
- **OS page cache cold**: The generation phase writes ~3.2 GB to Kafka log
  segments. The `sort-id` consumer is the first to read these segments back,
  paying the cost of faulting them into the page cache. Subsequent sorts
  (`name`, `continent`) benefit from warm page cache on the same `source`
  topic segments.
- **Kafka coordinator stabilization**: The first consumer group (`sort-id`)
  pays a one-time coordinator election and `__consumer_offsets` partition
  loading cost that subsequent groups skip (coordinator already warmed).

`Integer.compare` is cheaper per-comparison than `String.compareTo`, so the
comparison cost itself should favor id-sort — confirming that the overhead is
not algorithmic but environmental.

### 2. I/O Amplification: 24 GB Block I/O on a 3.2 GB Dataset

The measured `docker stats` block I/O was **24 GB read / 23.7 GB write** — a
~7.5× amplification over the raw dataset. This is expected given the
architecture:

| I/O Operation | Read | Write | Notes |
|---|---:|---:|---|
| Generation → `source` | — | ~3.2 GB | 50M records to 4 Kafka partitions |
| 3× sort consume `source` | 3 × 3.2 GB | — | Each sort re-reads all 50M records |
| 3× Phase A run spill | — | 3 × 3.2 GB | 39 run files per sort, ~82 MB each |
| 3× Phase B merge read | 3 × 3.2 GB | — | Reading all runs during k-way merge |
| 3× produce to output topic | — | 3 × 3.2 GB | 50M records to `id`/`name`/`continent` |
| Verification (4 count + 3 sort scans) | ~22.4 GB | — | 7 full-topic consumer scans |
| Kafka log segment writes | — | ~3.2 GB | Broker replicating to log segments |
| **Estimated total** | **~32 GB** | **~22 GB** | |

The measured 24 GB per direction is lower than the raw estimate because of
zstd compression on Kafka topics and OS page cache hits reducing physical disk
reads. The amplification is an inherent cost of the architecture: each sort
key requires a full consume-spill-merge-produce cycle, and verification
requires re-reading every topic.

### 3. Sequential Sort Execution

Running three sorts sequentially means the total sort wall time is roughly 3×
a single sort (~2758s total). Parallel execution would reduce this to ~1×
but would require 3 × 512 MB = 1.5 GB of sort heap, leaving only 500 MB for
Kafka + OS — not viable under the 2 GB budget.

### 4. Single-Partition Output Bottleneck

Each output topic has a single partition (required for global ordering), which
means the merge-phase producer is effectively single-threaded for output
writes. This creates a throughput ceiling on Phase B that does not exist for
the 4-partition `source` topic.

### 5. GC Pauses Under Small Heaps

With the sorter at `-Xmx512m` and the broker at `-Xmx384m`, both JVMs operate
near their heap ceilings during peak load. G1GC keeps pause times manageable
but cannot eliminate them entirely, especially during Phase A when the sort
buffer fills to capacity before each spill.

---

## Scaling Discussion

### Distributed Range-Partitioned Sort (TeraSort Approach)

For datasets larger than a single machine's memory, the sort can be distributed
using a **range-partitioned** strategy similar to Hadoop TeraSort:

1. **Sample Phase**: Read a small sample of keys to determine the data distribution.
2. **Range Partition**: Divide the key space into N non-overlapping ranges, one per
   worker node.
3. **Shuffle**: Route each record to the worker responsible for its key range.
4. **Local Sort**: Each worker sorts its partition independently (fully in-memory
   if the partition fits).
5. **Concatenate**: The globally sorted output is the concatenation of partitions
   0..N-1.

This achieves **O(N/P × log(N/P))** sort time where P = number of workers, with
the shuffle phase being the main bottleneck (network-bound).

### Spark / Flink

For production-scale sorted pipelines:

- **Apache Spark**: `rdd.sortByKey()` implements distributed sort with range
  partitioning automatically. Integrates with Kafka via structured streaming.
- **Apache Flink**: Provides event-time sorted windows and keyed state for
  continuous sorted output. Better for real-time sorted streams.

### Horizontal Kafka Scaling

- **More brokers**: Distribute partition leadership across brokers for higher
  aggregate throughput.
- **Higher replication factor**: `replication.factor=3` with `min.insync.replicas=2`
  for production durability.
- **Topic partitions**: More partitions on `source` for parallel consumption
  (output topics must remain single-partition for global ordering, or use
  range partitioning as above).

---

## Small-Scale Testing

Use the `RECORD_COUNT` environment variable to run quick validation tests:

```bash
# 100K records — completes in ~30 seconds
./docker/scripts/run.sh 100000

# 1M records — completes in ~2 minutes
./docker/scripts/run.sh 1000000

# 10M records — completes in ~15 minutes
./docker/scripts/run.sh 10000000
```

> [!TIP]
> Even at small scale, the sort algorithm creates multiple sorted runs (forced by
> buffer size limits) so that the k-way merge path is always exercised. This
> ensures the same code paths are validated regardless of record count.

---

## Technical Details

| Aspect           | Detail                                        |
|------------------|-----------------------------------------------|
| Language         | Java 17                                       |
| Build Tool       | Maven 3.9 (maven-shade-plugin for fat JAR)    |
| Kafka Version    | 3.7.1 (KRaft mode, no ZooKeeper)              |
| Docker           | Multi-stage build, slim JRE runtime            |
| Base Image       | `eclipse-temurin:17-jre-jammy`                 |
| Container Memory | 2 GB (`--memory=2g`)                           |
| Container CPUs   | 4 (`--cpus=4`)                                 |
| Compression      | zstd (all topics)                              |
| Sort Algorithm   | External merge sort (disk-spill + k-way merge) |

### Project Structure

```
KafkaSortPipeline/
├── pom.xml
├── README.md
├── src/
│   └── main/java/com/pipeline/
│       ├── generate/DataGeneratorApp.java
│       ├── sort/ExternalSortApp.java
│       └── verify/VerifyApp.java
├── docker/
│   ├── Dockerfile
│   ├── entrypoint.sh
│   ├── kafka-kraft.properties
│   └── scripts/
│       ├── build.sh
│       ├── run.sh
│       └── verify.sh
```

---

## License

This project is provided as-is for educational and demonstration purposes.
