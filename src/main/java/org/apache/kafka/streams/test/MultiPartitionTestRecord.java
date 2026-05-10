/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.test;

import org.apache.kafka.common.header.Headers;

import java.time.Instant;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A {@link TestRecord} extension that carries an explicit target partition,
 * implementing the {@code TestRecord.partition()} contract proposed by KIP-1238.
 *
 * <p>{@code MultiPartitionTopologyTestDriver} consults
 * {@link #partition()} to bypass key-hash routing when a non-{@code null} value
 * is provided. Pass {@code null} (or use a plain {@link TestRecord}) to fall
 * back to {@code Utils.murmur2(key) % n}, matching the production
 * {@code BuiltInPartitioner}.
 */
public class MultiPartitionTestRecord<K, V> extends TestRecord<K, V> {

    private final Integer partition;

    public MultiPartitionTestRecord(final K key,
                                    final V value,
                                    final Headers headers,
                                    final Instant recordTime,
                                    final Integer partition) {
        super(key, value, headers, recordTime);
        this.partition = partition;
    }

    public MultiPartitionTestRecord(final K key,
                                    final V value,
                                    final Headers headers,
                                    final Long timestampMs,
                                    final Integer partition) {
        super(key, value, headers, timestampMs);
        if (timestampMs != null && timestampMs < 0) {
            throw new IllegalArgumentException(
                String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestampMs));
        }
        this.partition = partition;
    }

    public MultiPartitionTestRecord(final K key, final V value, final Integer partition) {
        super(key, value);
        this.partition = partition;
    }

    /**
     * @return the explicit target partition, or {@code null} to defer to
     *         key-hash routing.
     */
    public Integer partition() {
        return partition;
    }

    /**
     * Reads the {@link #partition()} value from a {@link TestRecord} when it
     * is a {@code MultiPartitionTestRecord}, otherwise returns {@code null}.
     * Used by the driver to handle both record kinds uniformly.
     */
    public static Integer partitionOf(final TestRecord<?, ?> record) {
        if (record instanceof MultiPartitionTestRecord<?, ?> mp) {
            return mp.partition();
        }
        return null;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        if (!(o instanceof MultiPartitionTestRecord<?, ?> that)) {
            return false;
        }
        return Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), partition);
    }

    @Override
    public String toString() {
        final StringJoiner joiner = new StringJoiner(", ", MultiPartitionTestRecord.class.getSimpleName() + "[", "]")
            .add("key=" + key())
            .add("value=" + value())
            .add("headers=" + headers())
            .add("recordTime=" + getRecordTime());
        if (partition != null) {
            joiner.add("partition=" + partition);
        }
        return joiner.toString();
    }
}
