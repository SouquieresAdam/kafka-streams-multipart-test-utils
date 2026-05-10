# kafka-streams-multipart-test-utils

Catch Kafka Streams partitioning bugs in your unit tests, not on your prod migration.

A standalone library that brings **multi-partition support** to Kafka Streams'
`TopologyTestDriver`, implementing the contract described by
[KIP-1238](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1238%3A+Multipartition+for+TopologyTestDriver+in+Kafka+Streams).

## The problem this solves

Kafka Streams' standard `TopologyTestDriver` runs every record through a single fake task
with partition `0`. Tests pass with that fiction in place. Production then deploys the
same topology against topics with `N>1` partitions, and three classes of bug surface for
the first time:

1. **Routing fidelity** — a missing `selectKey`, a changed key serializer, a custom
   partitioner regression. Stock `TestRecord` has no partition field, so the test cannot
   assert routing. The bug appears in production.
2. **Silent state split** — two sources merged onto a shared store with mismatched
   partition counts collapse into one task under stock TTD, giving a misleading green.
   In production with `N>1`, the same key from each source side hashes to *different*
   store partitions and the per-key state silently splits.
3. **Partition count drift** — `Repartitioned.withNumberOfPartitions(N)` is invisible to
   stock TTD. A change from `4` to `8` (or removal) drifts undetected until the migration.

These bugs cost the most at the worst time: during a deploy, when the codebase has moved
on, the original author isn't on call, and rolling back doesn't undo the partial
state-store damage.

## What this library gives you

`MultiPartitionTopologyTestDriver` is a drop-in companion to `TopologyTestDriver` that
closes the gaps above:

- **One `StreamTask` per `(subtopologyId, partition)`**, not one task for the whole
  topology — so per-partition state actually exists at test time.
- **Production routing** via `Utils.murmur2(key) % n`, matching `BuiltInPartitioner`.
  Honours `TestRecord.partition()` for explicit-routing tests.
- **Repartition-topic partition counts** resolved by the 3-layer rule
  (explicit > inheritance > max upstream).
- **Per-partition state-store accessors:**
  `getStateStore(name, partition)`, `getStateStore(name, subtopologyId, partition)`,
  plus `partitionsOf(name)` to assert the topology's partitioning contract directly.
- **`MultiPartitionTestRecord`** — extends `TestRecord` with an `Integer partition` so
  output records carry the partition the production sink would produce to.

A worked-example project demonstrating all three risk exposures is at
[`kafka-streams-multipart-demo`](https://github.com/SouquieresAdam/kafka-streams-multipart-demo).
It runs the same topology under stock `TopologyTestDriver` and
`MultiPartitionTopologyTestDriver` and pins each gap as an assertable, CI-failable signal.

## Which version do I depend on?

Pick the row that matches the Kafka version your Streams app uses. The artifact is
published on Maven Central under `consulting.stream:kafka-streams-multipart-test-utils`.

| Your Kafka version | Depend on        | Source branch (info) |
|---|---|---|
| 3.7.x              | `3.7.2-kip1238`  | `kafka-3.7-3.9`  |
| 3.8.x              | `3.8.1-kip1238`  | `kafka-3.7-3.9`  |
| 3.9.x              | `3.9.2-kip1238`  | `kafka-3.7-3.9`  |
| 4.0.0 / 4.0.1      | `4.0.1-kip1238`  | `kafka-4.0-early` |
| 4.0.2              | `4.0.2-kip1238`  | `kafka-4.0`      |
| 4.1.x              | `4.1.2-kip1238`  | `kafka-4.1-4.2`  |
| 4.2.0              | `4.2.0-kip1238`  | `kafka-4.1-4.2`  |
| 4.4.0-SNAPSHOT (apache trunk) | not published — build from `main` and `publishToMavenLocal` | `main` |

Each artifact is the same library code compiled and tested against the exact Kafka
version in its name. The `Source branch` column is for maintainers; consumers only
care about the first two columns.

The package layout uses **split packages** (`org.apache.kafka.streams.*`) to access
package-private internals. Kafka has no `module-info.java`, so this works on the
classpath without `--add-opens`. Place this jar on the **classpath only** — on the
JPMS module path it would conflict with `kafka-streams` (two automatic modules
cannot export the same package).

Versions outside the table are **not supported**:
- 3.0.x – 3.6.x — Confluent Platform support window has lapsed; the backport surface
  (50+ compile errors at 3.0.2, 7 at 3.6.2) is not justified by demand.

## Branch model

`main` tracks `apache/trunk` and is the source of truth for the KIP-1238 multi-partition
logic. Each `kafka-<range>` branch is a sibling that re-bases the same logic on a stock
release (`stock 4.2.0` / `stock 3.9.2`) and patches the API drift documented in
`docs/COMPATIBILITY.md`. Bug fixes flow from `main` outward to the release branches via
cherry-pick.

## Usage

Pick the version from the table above for your Kafka. For example, if you're on
Kafka 3.9.2:

```xml
<dependency>
  <groupId>consulting.stream</groupId>
  <artifactId>kafka-streams-multipart-test-utils</artifactId>
  <version>3.9.2-kip1238</version>
  <scope>test</scope>
</dependency>
```

```java
import org.apache.kafka.streams.MultiPartitionTopologyTestDriver;
import org.apache.kafka.streams.test.MultiPartitionTestRecord;

MultiPartitionTopologyTestDriver driver = new MultiPartitionTopologyTestDriver(topology, props);
driver.declareTopic("input", 4);
driver.init();

driver.createInputTopic("input", keySerde.serializer(), valueSerde.serializer(), 4)
      .pipeInput(new MultiPartitionTestRecord<>("k", "v", null, Instant.now(), 2));

// The contract you cannot assert with stock TopologyTestDriver:
assertEquals(4, driver.partitionsOf("my-store"));
assertEquals(expectedValue, driver.getKeyValueStore("my-store", expectedPartition).get(key));
```

## Tests in this repo that pin the contract

The library's own test suite exercises each multi-partition behaviour on minimal
topologies. Highlights:

- `keyHashRoutingMatchesBuiltInPartitioner` — output partition matches production hash.
- `coAlignedKeysAggregateAcrossSourcesAfterRepartition` — `Repartitioned` aligns merged
  sources so the same key co-locates.
- `mismatchedPartitionsAcrossSharedStoreSourcesSilentlySplitState` — pins the silent
  state split so any future stricter validation is caught.
- `globalKTableJoinFedFromGlobalTaskIsVisibleToEveryActiveTask` — global + multi-task
  hand-off survives the partitioned runtime.
- `advanceWallClockTimeFiresPunctuatorsAcrossAllTasks` — wall-clock punctuation fans out
  to every task instance.

For an end-to-end stateful demo (DSL + `.process` + repartition + multi-sub-topology),
see [`kafka-streams-multipart-demo`](https://github.com/SouquieresAdam/kafka-streams-multipart-demo).

## Build

```bash
git checkout kafka-4.1-4.2             # pick the branch that matches your Kafka
./gradlew test                         # default kafkaVersion from gradle.properties
./gradlew test -PkafkaVersion=4.1.0    # cross-build inside the supported range
```

Requires JDK 25. The Gradle wrapper is committed at 9.4.1.

## Contributing

Implementation notes, source baseline, branch model conventions and a pre-push test
matrix live in [`CLAUDE.md`](CLAUDE.md). Per-version adapter detail lives in
[`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).

## Status

Pre-release. The implementation mirrors the
[`multi-partition-test-driver-v2` branch on `SouquieresAdam/kafka`](https://github.com/SouquieresAdam/kafka/tree/multi-partition-test-driver-v2);
once KIP-1238 lands upstream this artifact will be archived.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

This project is **not affiliated with, endorsed by, or sponsored by the Apache
Software Foundation**. "Apache", "Apache Kafka", and "Kafka" are trademarks of
the Apache Software Foundation; they are used here descriptively to indicate
compatibility.
