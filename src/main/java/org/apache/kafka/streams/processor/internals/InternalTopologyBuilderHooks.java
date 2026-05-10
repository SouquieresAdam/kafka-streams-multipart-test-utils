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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * External re-implementation of the three KIP-1238 hooks that
 * {@code MultiPartitionTopologyTestDriver} would otherwise need to add directly
 * to {@link InternalTopologyBuilder}. Lives in this package so it can read the
 * package-private fields of {@code TopicsInfo} and {@code Subtopology}.
 *
 * <p>Stock {@link InternalTopologyBuilder#subtopologyToTopicsInfo()} is already
 * {@code public}, so we do not need any reflection — we just iterate its result
 * with the same logic as the upstream patches.
 */
public final class InternalTopologyBuilderHooks {

    private InternalTopologyBuilderHooks() {
    }

    /**
     * @return a mapping from sub-topology id to the set of repartition source
     *         topic names declared by that sub-topology
     *         (the keys of {@code TopicsInfo.repartitionSourceTopics}).
     */
    public static Map<Integer, Set<String>> subtopologyToRepartitionTopics(final InternalTopologyBuilder builder) {
        final Map<Integer, Set<String>> result = new LinkedHashMap<>();
        for (final Map.Entry<Subtopology, TopicsInfo> entry : builder.subtopologyToTopicsInfo().entrySet()) {
            result.put(entry.getKey().nodeGroupId, new HashSet<>(entry.getValue().repartitionSourceTopics.keySet()));
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * @return the sub-topology id whose sink writes to {@code topic}, or
     *         {@code null} if no sub-topology has this topic among its sinks.
     */
    public static Integer subtopologyForRepartitionTopicProducer(final InternalTopologyBuilder builder,
                                                                 final String topic) {
        for (final Map.Entry<Subtopology, TopicsInfo> entry : builder.subtopologyToTopicsInfo().entrySet()) {
            if (entry.getValue().sinkTopics.contains(topic)) {
                return entry.getKey().nodeGroupId;
            }
        }
        return null;
    }

    /**
     * @return a mapping from each internal repartition topic name to its
     *         explicitly-configured number of partitions (e.g. via
     *         {@code Repartitioned.withNumberOfPartitions}). Topics whose count
     *         is left to upstream inheritance are absent from the map.
     */
    public static Map<String, Integer> explicitRepartitionTopicPartitionCounts(final InternalTopologyBuilder builder) {
        final Map<String, Integer> result = new HashMap<>();
        for (final TopicsInfo info : builder.subtopologyToTopicsInfo().values()) {
            for (final Map.Entry<String, InternalTopicConfig> entry : info.repartitionSourceTopics.entrySet()) {
                entry.getValue().numberOfPartitions().ifPresent(n -> result.put(entry.getKey(), n));
            }
        }
        return Collections.unmodifiableMap(result);
    }
}
