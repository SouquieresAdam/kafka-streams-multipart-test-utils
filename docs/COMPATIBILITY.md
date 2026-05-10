# Compatibility backport notes

This document tracks the API surface that diverges between branches and
the concrete adapter applied to each one. All branches share the same
KIP-1238 patch — what differs is the stock Kafka file the patch is
applied to and the per-version adapters layered on top.

## Source baseline

`MultiPartitionTopologyTestDriver` is built by taking the stock
`org.apache.kafka.streams.TopologyTestDriver` of the target Kafka
release and applying the KIP-1238 patch series from the
[`SouquieresAdam/kafka` fork](https://github.com/SouquieresAdam/kafka/tree/multi-partition-test-driver-v2)
(HEAD `7f38e72285`) on top via `patch --fuzz=10`.

`MultiPartitionTestInputTopic` and `MultiPartitionTestOutputTopic` are
forks of the corresponding stock classes (their package-private
constructors take `TopologyTestDriver`, so the renamed driver type
forces a fork). `MultiPartitionTestRecord` extends the stock
`TestRecord` to carry the explicit partition field.
`InternalTopologyBuilderHooks` is a split-package helper that re-derives
the three KIP-1238 hooks from the public
`InternalTopologyBuilder.subtopologyToTopicsInfo()` output, so no patch
on Kafka itself is required.

## Per-branch matrix

| Branch | Kafka range | Tested green | Adapter delta vs the next-newer branch |
|---|---|---|---|
| `main` | 4.4.0-SNAPSHOT (trunk) | local only | source baseline; needs trunk in `mavenLocal()` |
| `kafka-4.1-4.2` | 4.1.0 – 4.2.0 | 4.1.2, 4.2.0 | rebase on stock 4.2.0; restore 9-arg `ProcessorStateManager` (with `ChangelogRegister` + `stateUpdaterEnabled`); import `InternalTopologyBuilder.TopicsInfo` as nested |
| `kafka-4.0` | 4.0.2 only | 4.0.2 | `StreamTask.prepareCommit()` no-arg (vs `prepareCommit(boolean)` from 4.1) |
| `kafka-3.7-3.9` | 3.7.x – 3.9.x | 3.7.2, 3.8.1, 3.9.2 | rebase on stock 3.9.2; `StreamsConfig.defaultProductionExceptionHandler()` rename; `ProcessorStateManager.getStore()` / `GlobalStateManager.getStore()` rename (4.x dropped the `get` prefix); explicit imports for `RecordHeaders` and `ProcessorRecordContext`; `MultiPartitionTestOutputTopic` uses `driver.getQueueSize(topic)` instead of `driver.queueSize(topic)` |

Each `kafka-<range>` branch's `gradle.properties` pins the default
`kafkaVersion`. Cross-builds within the supported range are exercised
manually with `-PkafkaVersion=<x.y.z>` before tagging.

## Versions deliberately not covered

### 4.0.0 / 4.0.1

Two divergences from 4.0.2:

1. `StreamsMetricsImpl(Metrics, String, String, Time)` — 4-arg form
   that collapsed to 3-arg in 4.0.2. Pure compile fix.
2. `StreamTask.committableOffsetsAndMetadata` calls
   `findOffsetAndMetadata` for every input partition; on 4.0.0/4.0.1 the
   global-table partitions are not yet pre-registered when the multi-sub
   runtime calls `prepareCommit`, so it throws
   *"Stream task X_Y does not know the partition: …"*. 9 of 16 tests
   fail at runtime, including all GlobalKTable scenarios. This is a
   behavioural divergence, not just an API rename, and would require a
   separate registration step in the multi-sub setup path.

A user pinned to 4.0.0/4.0.1 should upgrade to 4.0.2 (drop-in,
patch-level Kafka release).

### 3.0.x – 3.6.x

Below the Confluent Platform support window. Quick survey of compile
errors when `kafka-3.7-3.9` is run against these versions:

| Target | Errors | Sample |
|---|---|---|
| 3.6.2 | 7 | `StateStore.init(StateStoreContext)` was abstract; subsequent classes diverge |
| 3.5.2 | 7 | similar to 3.6 |
| 3.4.1 | 15 | `RecordCollector` ctor / `StreamsProducer` lifecycle changes |
| 3.3.2 | 19 | additional `ProcessorContext` API churn |
| 3.2.3 | 22 | `Stores.persistentTimestampedKeyValueStore` signature |
| 3.1.2 | 47 | broad surface drift (`Cache`, `RecordQueue`) |
| 3.0.2 | 50 | as 3.1, plus `KafkaProducer.transactionId()` etc. |

These targets are tractable but the cost-to-benefit ratio is poor: they
are out of vendor support and the test surface that would actually
exercise multi-partition behaviour against them is small. A user who
needs them should upgrade Kafka — the API differences are far broader
than KIP-1238 itself.

## How to build

The default `gradle.properties` on each branch picks a sensible Kafka
version for that branch. Override with `-PkafkaVersion=<x.y.z>` if you
need to cross-build within the supported range.

`main` requires the upstream Kafka fork (or any 4.4.0-SNAPSHOT clone
sitting on the multi-partition source) installed locally:

```sh
cd /path/to/kafka
./gradlew -x test -x spotbugsMain -x spotbugsTest \
    :clients:publishToMavenLocal \
    :streams:publishToMavenLocal \
    :streams:test-utils:publishToMavenLocal
```

The release branches resolve everything from Maven Central.
