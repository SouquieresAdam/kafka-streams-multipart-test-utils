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

| Branch | Kafka Streams range | Tested green | Notes |
|---|---|---|---|
| `main` | 4.4.0-SNAPSHOT (apache trunk) | local only | requires the upstream Kafka fork in `mavenLocal()` |
| `kafka-4.1-4.2` | 4.1.0 – 4.2.0 | 4.1.2, 4.2.0 (16/16) | published Maven Central deps; default branch for Kafka 4.x users |
| `kafka-4.0` | 4.0.2 only | 4.0.2 (16/16) | 3-arg `StreamsMetricsImpl` ctor |
| `kafka-4.0-early` | 4.0.0 – 4.0.1 | 4.0.0, 4.0.1 (16/16) | 4-arg ctor; replays `task.updateNextOffsets()` to dodge pre-4.0.2 strict commit |
| `kafka-3.7-3.9` | 3.7.x – 3.9.x | 3.7.2, 3.8.1, 3.9.2 (16/16) | lower bound of CFLT support; no plans to backport further |

The package layout uses **split packages** (`org.apache.kafka.streams.*`) to
access package-private internals. Kafka has no `module-info.java`, so this
works on the classpath without `--add-opens`. Each branch produces an
artefact tagged `<kafkaVersion>-kip1238`.

Versions outside the table are **not supported**:
- 3.0.x – 3.6.x — Confluent Platform support window has lapsed; the
  backport surface (50+ compile errors at 3.0.2, 7 at 3.6.2) is not
  justified by demand.

## Branch model

`main` tracks `apache/trunk` and is the source of truth for the
KIP-1238 multi-partition logic. Each `kafka-<range>` branch is a sibling
that re-bases the same logic on a stock release (`stock 4.2.0` /
`stock 3.9.2`) and patches the API drift documented in
`docs/COMPATIBILITY.md`. Bug fixes flow from `main` outward to the
release branches via cherry-pick.

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
git checkout kafka-4.1-4.2             # pick the branch that matches your Kafka
./gradlew test                         # default kafkaVersion from gradle.properties
./gradlew test -PkafkaVersion=4.1.0    # cross-build inside the supported range
```

Requires JDK 25. The Gradle wrapper is committed at 9.4.1.

## Contributing

Implementation notes, source baseline, branch model conventions and
a pre-push test matrix live in [`CLAUDE.md`](CLAUDE.md). Per-version
adapter detail lives in [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).

## Status

Pre-release. The implementation mirrors the
[`multi-partition-test-driver-v2` branch on `SouquieresAdam/kafka`](https://github.com/SouquieresAdam/kafka/tree/multi-partition-test-driver-v2);
once KIP-1238 lands upstream this artifact will be archived.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE).
