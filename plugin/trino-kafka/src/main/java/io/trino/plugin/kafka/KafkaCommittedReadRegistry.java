/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kafka;

import com.google.common.cache.Cache;
import com.google.inject.Inject;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;

import java.util.concurrent.TimeUnit;

import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static java.lang.String.format;

public class KafkaCommittedReadRegistry
{
    private final Cache<TrackedScanKey, Boolean> trackedScans = EvictableCacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();

    @Inject
    public KafkaCommittedReadRegistry() {}

    public void register(String queryId, String groupId, String topicName)
    {
        TrackedScanKey key = new TrackedScanKey(queryId, groupId, topicName);
        if (trackedScans.asMap().putIfAbsent(key, Boolean.TRUE) != null) {
            throw new TrinoException(
                    KAFKA_SPLIT_ERROR,
                    format("Committed-read mode does not allow multiple tracked scans of topic '%s' with group ID '%s' in query '%s'", topicName, groupId, queryId));
        }
    }

    public void cleanupQuery(String queryId)
    {
        trackedScans.asMap().keySet().removeIf(key -> key.queryId().equals(queryId));
    }

    private record TrackedScanKey(String queryId, String groupId, String topicName) {}
}
