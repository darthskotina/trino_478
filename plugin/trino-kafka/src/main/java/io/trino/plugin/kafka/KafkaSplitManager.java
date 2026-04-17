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
import com.google.common.math.LongMath;
import com.google.inject.Inject;
import io.trino.plugin.kafka.schema.ContentSchemaProvider;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.OFFSET_TIMESTAMP_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.PARTITION_ID_FIELD;
import static io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId.PARTITION_OFFSET_FIELD;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.expression.Constant.TRUE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private final KafkaConsumerFactory consumerFactory;
    private final KafkaFilterManager kafkaFilterManager;
    private final KafkaInternalFieldManager kafkaInternalFieldManager;
    private final ContentSchemaProvider contentSchemaProvider;
    private final int messagesPerSplit;

    @Inject
    public KafkaSplitManager(
            KafkaConsumerFactory consumerFactory,
            KafkaConfig kafkaConfig,
            KafkaFilterManager kafkaFilterManager,
            KafkaInternalFieldManager kafkaInternalFieldManager,
            ContentSchemaProvider contentSchemaProvider)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.messagesPerSplit = kafkaConfig.getMessagesPerSplit();
        this.kafkaFilterManager = requireNonNull(kafkaFilterManager, "kafkaFilterManager is null");
        this.kafkaInternalFieldManager = requireNonNull(kafkaInternalFieldManager, "kafkaInternalFieldManager is null");
        this.contentSchemaProvider = requireNonNull(contentSchemaProvider, "contentSchemaProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) table;
        enforceFilterRequirement(session, kafkaTableHandle, constraint);
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerFactory.create(session)) {
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(kafkaTableHandle.topicName());

            List<TopicPartition> topicPartitions = partitionInfos.stream()
                    .map(KafkaSplitManager::toTopicPartition)
                    .collect(toImmutableList());

            Map<TopicPartition, Long> partitionBeginOffsets = kafkaConsumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> partitionEndOffsets = kafkaConsumer.endOffsets(topicPartitions);
            KafkaFilteringResult kafkaFilteringResult = kafkaFilterManager.getKafkaFilterResult(session, kafkaTableHandle,
                    partitionInfos, partitionBeginOffsets, partitionEndOffsets);
            partitionInfos = kafkaFilteringResult.partitionInfos();
            partitionBeginOffsets = kafkaFilteringResult.partitionBeginOffsets();
            partitionEndOffsets = kafkaFilteringResult.partitionEndOffsets();
            enforceMaxReadOffsets(
                    session,
                    kafkaTableHandle,
                    partitionInfos.stream()
                            .map(KafkaSplitManager::toTopicPartition)
                            .collect(toImmutableList()),
                    partitionBeginOffsets,
                    partitionEndOffsets);

            ImmutableList.Builder<KafkaSplit> splits = ImmutableList.builder();
            Optional<String> keyDataSchemaContents = contentSchemaProvider.getKey(kafkaTableHandle);
            Optional<String> messageDataSchemaContents = contentSchemaProvider.getMessage(kafkaTableHandle);

            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = toTopicPartition(partitionInfo);
                HostAddress leader = HostAddress.fromParts(partitionInfo.leader().host(), partitionInfo.leader().port());
                new Range(partitionBeginOffsets.get(topicPartition), partitionEndOffsets.get(topicPartition))
                        .partition(messagesPerSplit).stream()
                        .map(range -> new KafkaSplit(
                                kafkaTableHandle.topicName(),
                                kafkaTableHandle.keyDataFormat(),
                                kafkaTableHandle.messageDataFormat(),
                                keyDataSchemaContents,
                                messageDataSchemaContents,
                                partitionInfo.partition(),
                                range,
                                leader))
                        .forEach(splits::add);
            }
            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof TrinoException) {
                throw e;
            }
            throw new TrinoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.tableName(), kafkaTableHandle.topicName()), e);
        }
    }

    private static TopicPartition toTopicPartition(PartitionInfo partitionInfo)
    {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }

    private void enforceFilterRequirement(ConnectorSession session, KafkaTableHandle kafkaTableHandle, Constraint constraint)
    {
        if (!KafkaSessionProperties.isRequireFilter(session) || hasFilter(constraint)) {
            return;
        }

        throw new TrinoException(
                QUERY_REJECTED,
                format(
                        "Reading Kafka topic '%s' requires a WHERE clause. Use a predicate on %s, %s, or %s to bound the scan.",
                        kafkaTableHandle.topicName(),
                        internalFieldName(PARTITION_ID_FIELD),
                        internalFieldName(PARTITION_OFFSET_FIELD),
                        internalFieldName(OFFSET_TIMESTAMP_FIELD)));
    }

    private void enforceMaxReadOffsets(
            ConnectorSession session,
            KafkaTableHandle kafkaTableHandle,
            List<TopicPartition> topicPartitions,
            Map<TopicPartition, Long> partitionBeginOffsets,
            Map<TopicPartition, Long> partitionEndOffsets)
    {
        long maxReadOffsets = KafkaSessionProperties.getMaxReadOffsets(session);
        if (maxReadOffsets == 0) {
            return;
        }

        long estimatedReadOffsets = estimateReadOffsets(topicPartitions, partitionBeginOffsets, partitionEndOffsets);
        if (estimatedReadOffsets <= maxReadOffsets) {
            return;
        }

        throw new TrinoException(
                QUERY_REJECTED,
                format(
                        "Reading Kafka topic '%s' would scan %s offsets, which exceeds the configured limit of %s. Add a tighter predicate on %s, %s, or %s, or raise kafka.max_read_offsets for this session.",
                        kafkaTableHandle.topicName(),
                        estimatedReadOffsets,
                        maxReadOffsets,
                        internalFieldName(PARTITION_ID_FIELD),
                        internalFieldName(PARTITION_OFFSET_FIELD),
                        internalFieldName(OFFSET_TIMESTAMP_FIELD)));
    }

    static boolean hasFilter(Constraint constraint)
    {
        return !constraint.getSummary().isAll()
                || !constraint.getExpression().equals(TRUE)
                || constraint.getPredicateColumns()
                        .map(predicateColumns -> !predicateColumns.isEmpty())
                        .orElse(false);
    }

    static long estimateReadOffsets(
            List<TopicPartition> topicPartitions,
            Map<TopicPartition, Long> partitionBeginOffsets,
            Map<TopicPartition, Long> partitionEndOffsets)
    {
        long totalOffsets = 0;
        for (TopicPartition topicPartition : topicPartitions) {
            long beginOffset = partitionBeginOffsets.get(topicPartition);
            long endOffset = partitionEndOffsets.get(topicPartition);
            totalOffsets = LongMath.saturatedAdd(totalOffsets, Long.max(0, endOffset - beginOffset));
        }
        return totalOffsets;
    }

    private String internalFieldName(KafkaInternalFieldManager.InternalFieldId internalFieldId)
    {
        return kafkaInternalFieldManager.getFieldById(internalFieldId).getColumnName();
    }
}
