# Compatibility backport notes

This document tracks the API surface that diverges between
`apache/kafka:trunk` (Kafka `4.4.0-SNAPSHOT`, where the multi-partition
fork currently lives) and each published Kafka release we want to target.
Each item below is a concrete site to adapt — most are constructor
signature changes or methods that simply did not exist yet.

## Source baseline

`MultiPartitionTopologyTestDriver`, `MultiPartitionTestInputTopic`,
`MultiPartitionTestOutputTopic`, `MultiPartitionTestRecord` and
`InternalTopologyBuilderHooks` were all lifted from the
[`SouquieresAdam/kafka` fork](https://github.com/SouquieresAdam/kafka/tree/multi-partition-test-driver-v2),
HEAD `7f38e72285`.

That fork is rebased on `apache/kafka` at version `4.4.0-SNAPSHOT`. Any
divergence below is therefore a backport need, not a forward port.

## Trunk-only types removed for backports

The following imports/usages exist in the source today and need to be
removed (or guarded behind an adapter) when targeting any released Kafka:

| Symbol | Site (line ≈) | Owning area |
|---|---|---|
| `AggregationWithHeaders` | imports + facade signatures | KIP-1153 (with-headers state stores) |
| `SessionStoreWithHeaders` | imports + facade | KIP-1153 |
| `TimestampedKeyValueStoreWithHeaders` | imports + facade | KIP-1153 |
| `TimestampedWindowStoreWithHeaders` | imports + facade | KIP-1153 |
| `ValueTimestampHeaders` | imports | KIP-1153 |
| `GenericReadOnlyKeyValueStoreFacade` | imports + facade construction | trunk-only refactor |
| `GenericReadOnlyWindowStoreFacade` | imports + facade construction | trunk-only refactor |
| `SessionStoreIteratorFacade` | imports | trunk-only refactor |
| `ValueConverters` | imports + facade conversion calls | trunk-only refactor |

All of the above need to be either deleted (when the corresponding
public API is also missing in target Kafka) or replaced with the
equivalent public API of the target release.

## Constructor / method signature changes

| API | Trunk | Published 4.x (most recent) | 3.x range |
|---|---|---|---|
| `ProcessorStateManager(...)` | 7 args, no `ChangelogRegister`, no `stateUpdaterEnabled` | varies; see `bdca6af8c9` in upstream branch for the 4.x→trunk diff | 9-arg form with `ChangelogRegister`, `stateUpdaterEnabled` |
| `GlobalStateUpdateTask(...)` | trunk-specific signature | needs side-by-side comparison | older signature missing `ProcessingExceptionHandler` etc. |
| `StreamTask.prepareCommit()` | `prepareCommit(boolean)` | `prepareCommit(boolean)` (4.x) | `prepareCommit()` no-arg (older 3.x) |
| `StateStore.commit(Map<TopicPartition, Long>)` | default method present | absent in 4.2.0 and below | absent |
| `StateStore.committedOffset(TopicPartition)` | default method present | absent | absent |
| `StateStore.managesOffsets()` | default method present | absent | absent |

The 3 facade overrides for `commit`/`committedOffset`/`managesOffsets`
have already been deleted from `MultiPartitionTopologyTestDriver` —
that is a partial 4.2 backport step, kept on `main` because the inherited
default behaviour (`flush()`) is acceptable for test-driver semantics.

## Backport plan, ordered shortest path first

1. **Target 4.2.0** (latest published) by removing the KIP-1153 facades
   and providing reflection or version-detection shims for the
   `ProcessorStateManager` and `GlobalStateUpdateTask` constructors.
2. **Target 4.1.x** — usually adds back the previous-form constructors;
   small delta from 4.2.
3. **Target 4.0.x** — `kafka-clients` API drift; check
   `MockConsumer`/`MockProducer` constructor signatures.
4. **Target 3.7+** — `ProcessorStateManager` regains
   `ChangelogRegister` + `stateUpdaterEnabled`. Add a 3.x-specific
   source set under `src/main/java-3.x/`.
5. **Target 3.0+** — earliest support; `StateStoreContext` was added in
   3.x, ensure the test driver does not assume newer interface methods.

Each target produces a published artefact tagged
`<kafka.version>-kip1238` (see `gradle.properties`).

## How to run the build today

The default `kafkaVersion=4.4.0-SNAPSHOT` requires the upstream Kafka
fork to be installed in the local Maven cache:

```sh
cd /c/repository/kafka
./gradlew -x test -x spotbugsMain -x spotbugsTest \
    :clients:publishToMavenLocal \
    :streams:publishToMavenLocal \
    :streams:test-utils:publishToMavenLocal
```

Then from this repo:

```sh
./gradlew test
```

Cross-builds (`-PkafkaVersion=4.2.0` etc.) will fail until the
corresponding backport variant lands on this repo.
