# kafka-streams-multipart-test-utils

A standalone library that brings **multi-partition support** to Kafka Streams'
`TopologyTestDriver`, implementing the contract described by
[KIP-1238](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1238%3A+Multipartition+for+TopologyTestDriver+in+Kafka+Streams).

While KIP-1238 progresses upstream, this library lets you exercise multi-partition
topologies in unit tests today, without forking Kafka.

## What it gives you

- `MultiPartitionTopologyTestDriver` — a drop-in companion to `TopologyTestDriver`
  that:
  - Honours `TestRecord.partition()` for explicit routing
  - Routes by `Utils.murmur2(key) % n` (matches the production `BuiltInPartitioner`)
  - Materialises one `StreamTask` per `(subtopologyId, partition)`
  - Resolves repartition-topic partition counts via the 3-layer rule
    (explicit > inheritance > max upstream)
  - Exposes partition-aware state-store accessors:
    `getStateStore(name, partition)`, `getStateStore(name, subtopologyId, partition)`
- `MultiPartitionTestRecord` — extends `TestRecord` with an `Integer partition` field

## Compatibility matrix

| Kafka Streams | Status | Notes |
|---|---|---|
| 4.4.0-SNAPSHOT (apache trunk) | working source baseline | the fork was lifted directly from this revision |
| 4.2.0 | backport in progress | requires removing trunk-only APIs (`AggregationWithHeaders`, `SessionStoreWithHeaders`, `GenericReadOnlyKeyValueStoreFacade`, `ValueConverters`) and adapting `ProcessorStateManager`/`GlobalStateUpdateTask` constructors |
| 4.1.x | planned | inherits 4.2 backport + small adapter tweaks |
| 4.0.x | planned | larger constructor changes |
| 3.5.x – 3.9.x | planned | requires reflection adapters for state-store APIs |
| 3.0.x – 3.4.x | exploratory | many `StateStore` API gaps; may need a second source set |

The package layout uses **split packages** (`org.apache.kafka.streams.*`) to access
package-private internals. Kafka has no `module-info.java`, so this works on the
classpath without `--add-opens`. Each Kafka minor version that touches those
internals (`InternalTopologyBuilder`, `StreamTask`, `ProcessorStateManager`,
`ProcessorContextImpl`) requires a dedicated build variant.

## Status (work in progress)

This repo currently contains the **structure** of the standalone library. The
sources are a verbatim port of the
[`multi-partition-test-driver-v2`](https://github.com/SouquieresAdam/kafka/tree/multi-partition-test-driver-v2)
branch, which sits on top of `apache/trunk` (Kafka `4.4.0-SNAPSHOT`). As such,
they will **not** compile against any published Kafka release as-is. The
backport effort tracked in `docs/COMPATIBILITY.md` enumerates the exact API
sites that need adapters per Kafka minor version.

## Usage

```xml
<dependency>
  <groupId>consulting.stream</groupId>
  <artifactId>kafka-streams-multipart-test-utils</artifactId>
  <version>${kafka.version}-kip1238</version>
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
```

## Build

```bash
./gradlew test                        # default Kafka version
./gradlew test -PkafkaVersion=4.1.0   # cross-build against another minor
```

Requires JDK 25.

## Status

Pre-release. The implementation mirrors the
[`multi-partition-test-driver-v2` branch on `SouquieresAdam/kafka`](https://github.com/SouquieresAdam/kafka/tree/multi-partition-test-driver-v2);
once KIP-1238 lands upstream this artifact will be archived.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE).
