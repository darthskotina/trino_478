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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static java.lang.String.format;
import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;

public class KafkaOffsetBoundsService
{
    private final KafkaConsumerFactory consumerFactory;
    private final TableDescriptionSupplier tableDescriptionSupplier;

    @Inject
    public KafkaOffsetBoundsService(KafkaConsumerFactory consumerFactory, TableDescriptionSupplier tableDescriptionSupplier)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
    }

    public Optional<KafkaTopicDescription> getTopicDescription(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return tableDescriptionSupplier.getTopicDescription(session, schemaTableName);
    }

    public List<KafkaOffsetBounds> getOffsetBounds(ConnectorSession session, SchemaTableName schemaTableName, String topicName, Optional<Integer> partition)
    {
        TopicPartitionOffsets topicPartitionOffsets = getTopicPartitionOffsets(session, topicName, partition);
        return topicPartitionOffsets.partitionInfos().stream()
                .map(partitionInfo -> {
                    TopicPartition topicPartition = toTopicPartition(partitionInfo);
                    return new KafkaOffsetBounds(
                            partitionInfo.partition(),
                            topicPartitionOffsets.beginningOffsets().get(topicPartition),
                            topicPartitionOffsets.endOffsets().get(topicPartition));
                })
                .collect(toImmutableList());
    }

    public TopicPartitionOffsets getTopicPartitionOffsets(ConnectorSession session, String topicName, Optional<Integer> partition)
    {
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerFactory.create(session)) {
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topicName).stream()
                    .sorted(comparingInt(PartitionInfo::partition))
                    .collect(toImmutableList());
            List<PartitionInfo> requestedPartitions = getRequestedPartitions(topicName, partitionInfos, partition);
            List<TopicPartition> topicPartitions = requestedPartitions.stream()
                    .map(KafkaOffsetBoundsService::toTopicPartition)
                    .collect(toImmutableList());
            return new TopicPartitionOffsets(
                    requestedPartitions,
                    kafkaConsumer.beginningOffsets(topicPartitions),
                    kafkaConsumer.endOffsets(topicPartitions));
        }
        catch (Exception e) {
            if (e instanceof TrinoException) {
                throw e;
            }
            throw new TrinoException(KAFKA_SPLIT_ERROR, format("Failed to fetch offset bounds for topic '%s'", topicName), e);
        }
    }

    private static List<PartitionInfo> getRequestedPartitions(String topicName, List<PartitionInfo> partitionInfos, Optional<Integer> partition)
    {
        if (partition.isEmpty()) {
            return partitionInfos;
        }

        return partitionInfos.stream()
                .filter(partitionInfo -> partitionInfo.partition() == partition.get())
                .findFirst()
                .map(ImmutableList::of)
                .orElseThrow(() -> new TrinoException(
                        KAFKA_SPLIT_ERROR,
                        format("Partition %s does not exist for topic '%s'", partition.get(), topicName)));
    }

    static TopicPartition toTopicPartition(PartitionInfo partitionInfo)
    {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }

    public record TopicPartitionOffsets(
            List<PartitionInfo> partitionInfos,
            Map<TopicPartition, Long> beginningOffsets,
            Map<TopicPartition, Long> endOffsets)
    {
        public TopicPartitionOffsets
        {
            partitionInfos = ImmutableList.copyOf(requireNonNull(partitionInfos, "partitionInfos is null"));
            requireNonNull(beginningOffsets, "beginningOffsets is null");
            requireNonNull(endOffsets, "endOffsets is null");
        }
    }
}
