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
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static io.trino.plugin.kafka.KafkaFilterManager.filterRangeByDomain;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private final KafkaConsumerFactory consumerFactory;
    private final KafkaAdminFactory adminFactory;
    private final KafkaFilterManager kafkaFilterManager;
    private final ContentSchemaProvider contentSchemaProvider;
    private final KafkaCommittedReadRegistry committedReadRegistry;
    private final KafkaOffsetBoundsService offsetBoundsService;
    private final int messagesPerSplit;
    private final KafkaCommittedReadMissingOffsetPolicy missingOffsetPolicy;

    @Inject
    public KafkaSplitManager(
            KafkaConsumerFactory consumerFactory,
            KafkaAdminFactory adminFactory,
            KafkaConfig kafkaConfig,
            KafkaFilterManager kafkaFilterManager,
            ContentSchemaProvider contentSchemaProvider,
            KafkaCommittedReadRegistry committedReadRegistry,
            KafkaOffsetBoundsService offsetBoundsService)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.adminFactory = requireNonNull(adminFactory, "adminFactory is null");
        this.messagesPerSplit = kafkaConfig.getMessagesPerSplit();
        this.kafkaFilterManager = requireNonNull(kafkaFilterManager, "kafkaFilterManager is null");
        this.contentSchemaProvider = requireNonNull(contentSchemaProvider, "contentSchemaProvider is null");
        this.committedReadRegistry = requireNonNull(committedReadRegistry, "committedReadRegistry is null");
        this.offsetBoundsService = requireNonNull(offsetBoundsService, "offsetBoundsService is null");
        this.missingOffsetPolicy = requireNonNull(kafkaConfig.getCommittedReadMissingOffsetPolicy(), "missingOffsetPolicy is null");
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
        boolean committedReadMode = KafkaSessionProperties.isCommittedReadEnabled(session);
        boolean allowCommittedReadOffsetRewind = committedReadMode && KafkaSessionProperties.isCommittedReadAllowOffsetRewind(session);
        Optional<String> committedReadGroupId = committedReadMode ? Optional.of(KafkaSessionProperties.getRequiredCommittedReadGroupId(session)) : Optional.empty();

        committedReadGroupId.ifPresent(groupId -> committedReadRegistry.register(session.getQueryId(), groupId, kafkaTableHandle.topicName()));

        try {
            KafkaOffsetBoundsService.TopicPartitionOffsets topicPartitionOffsets = offsetBoundsService.getTopicPartitionOffsets(session, kafkaTableHandle.topicName(), Optional.empty());
            List<PartitionInfo> partitionInfos = topicPartitionOffsets.partitionInfos();
            Map<TopicPartition, Long> partitionBeginOffsets = topicPartitionOffsets.beginningOffsets();
            Map<TopicPartition, Long> partitionEndOffsets = topicPartitionOffsets.endOffsets();
            Map<TopicPartition, Long> partitionLogStartOffsets = partitionBeginOffsets;
            Map<TopicPartition, Long> partitionLogEndOffsets = partitionEndOffsets;
            KafkaFilteringResult kafkaFilteringResult = kafkaFilterManager.getKafkaFilterResult(session, kafkaTableHandle,
                    partitionInfos, partitionBeginOffsets, partitionEndOffsets, committedReadMode);
            partitionInfos = kafkaFilteringResult.partitionInfos();
            partitionBeginOffsets = kafkaFilteringResult.partitionBeginOffsets();
            partitionEndOffsets = kafkaFilteringResult.partitionEndOffsets();
            Optional<Long> explicitPartitionOffsetLowerBound = allowCommittedReadOffsetRewind ? getExplicitPartitionOffsetLowerBound(kafkaTableHandle) : Optional.empty();

            ImmutableList.Builder<KafkaSplit> splits = ImmutableList.builder();
            Optional<String> keyDataSchemaContents = contentSchemaProvider.getKey(kafkaTableHandle);
            Optional<String> messageDataSchemaContents = contentSchemaProvider.getMessage(kafkaTableHandle);
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = committedReadGroupId
                    .map(groupId -> getCommittedOffsets(session, groupId))
                    .orElse(Map.of());

            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = KafkaOffsetBoundsService.toTopicPartition(partitionInfo);
                HostAddress leader = HostAddress.fromParts(partitionInfo.leader().host(), partitionInfo.leader().port());
                if (!committedReadMode) {
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
                                    Optional.empty(),
                                    leader))
                            .forEach(splits::add);
                    continue;
                }

                SplitPlanningResult splitPlanningResult = planCommittedReadSplit(
                        kafkaTableHandle,
                        topicPartition,
                        partitionLogStartOffsets.get(topicPartition),
                        partitionLogEndOffsets.get(topicPartition),
                        explicitPartitionOffsetLowerBound.map(offset -> max(partitionLogStartOffsets.get(topicPartition), offset)).orElse(partitionBeginOffsets.get(topicPartition)),
                        partitionEndOffsets.get(topicPartition),
                        allowCommittedReadOffsetRewind,
                        explicitPartitionOffsetLowerBound.isPresent(),
                        committedReadGroupId.orElseThrow(),
                        committedOffsets.get(topicPartition));
                splitPlanningResult.split().ifPresent(rangeAndMetadata -> splits.add(new KafkaSplit(
                        kafkaTableHandle.topicName(),
                        kafkaTableHandle.keyDataFormat(),
                        kafkaTableHandle.messageDataFormat(),
                        keyDataSchemaContents,
                        messageDataSchemaContents,
                        partitionInfo.partition(),
                        rangeAndMetadata.range(),
                        Optional.of(rangeAndMetadata.metadata()),
                        leader)));
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

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableFunctionHandle function)
    {
        if (function instanceof KafkaOffsetBoundsFunctionHandle offsetBoundsHandle) {
            return new FixedSplitSource(offsetBoundsService.getOffsetBounds(session, offsetBoundsHandle.schemaTableName(), offsetBoundsHandle.topicName(), offsetBoundsHandle.partition())
                    .stream()
                    .map(KafkaOffsetBoundsSplit::new)
                    .collect(toImmutableList()));
        }
        throw new UnsupportedOperationException("Unrecognized function: " + function);
    }

    private static Optional<Long> getExplicitPartitionOffsetLowerBound(KafkaTableHandle tableHandle)
    {
        return tableHandle.constraint()
                .getDomains()
                .flatMap(domains -> domains.entrySet().stream()
                        .filter(entry -> ((KafkaColumnHandle) entry.getKey()).getName().equals("_partition_offset"))
                        .map(Map.Entry::getValue)
                        .findFirst())
                .flatMap(KafkaSplitManager::getExplicitLowerBound);
    }

    private static Optional<Long> getExplicitLowerBound(Domain domain)
    {
        return filterRangeByDomain(domain)
                .map(Range::begin)
                .filter(begin -> begin >= 0);
    }

    private Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets(ConnectorSession session, String groupId)
    {
        try (Admin admin = adminFactory.create(session)) {
            return admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get();
        }
        catch (Exception e) {
            throw new TrinoException(KAFKA_SPLIT_ERROR, format("Failed to fetch committed offsets for group ID '%s'", groupId), e);
        }
    }

    private SplitPlanningResult planCommittedReadSplit(
            KafkaTableHandle tableHandle,
            TopicPartition topicPartition,
            long logStart,
            long logEnd,
            long filteredBegin,
            long filteredEnd,
            boolean allowOffsetRewind,
            boolean explicitOffsetLowerBound,
            String groupId,
            OffsetAndMetadata committedOffsetMetadata)
    {
        CommittedOffsetResolution resolution = resolveCommittedOffset(tableHandle, topicPartition, logStart, logEnd, groupId, committedOffsetMetadata);
        boolean explicitRewind = allowOffsetRewind && explicitOffsetLowerBound;
        long start = explicitRewind ? filteredBegin : min(filteredEnd, resolution.committedBase());
        long end = filteredEnd;

        if (start < end) {
            return new SplitPlanningResult(Optional.of(new SplitRangeAndMetadata(
                    new Range(start, end),
                    new KafkaCommittedReadSplitMetadata(groupId, false, end))));
        }
        if (!explicitRewind && resolution.missingOrInvalid() && resolution.appliedPolicy().orElse(null) == KafkaCommittedReadMissingOffsetPolicy.LATEST) {
            long checkpointTarget = clampCheckpointTarget(logStart, logEnd, filteredEnd);
            return new SplitPlanningResult(Optional.of(new SplitRangeAndMetadata(
                    new Range(checkpointTarget, checkpointTarget),
                    new KafkaCommittedReadSplitMetadata(groupId, true, checkpointTarget))));
        }
        return new SplitPlanningResult(Optional.empty());
    }

    private CommittedOffsetResolution resolveCommittedOffset(
            KafkaTableHandle tableHandle,
            TopicPartition topicPartition,
            long logStart,
            long logEnd,
            String groupId,
            OffsetAndMetadata committedOffsetMetadata)
    {
        if (committedOffsetMetadata != null) {
            long committedOffset = committedOffsetMetadata.offset();
            if (committedOffset >= logStart && committedOffset <= logEnd) {
                return new CommittedOffsetResolution(committedOffset, false, Optional.empty());
            }
            if (missingOffsetPolicy == KafkaCommittedReadMissingOffsetPolicy.ERROR) {
                throw new TrinoException(
                        KAFKA_SPLIT_ERROR,
                        format(
                                "Committed offset %s for topic '%s' partition %s and group ID '%s' is outside the current broker range [%s, %s]",
                                committedOffset,
                                tableHandle.topicName(),
                                topicPartition.partition(),
                                groupId,
                                logStart,
                                logEnd));
            }
            return new CommittedOffsetResolution(applyMissingOffsetPolicy(logStart, logEnd), true, Optional.of(missingOffsetPolicy));
        }

        if (missingOffsetPolicy == KafkaCommittedReadMissingOffsetPolicy.ERROR) {
            throw new TrinoException(
                    KAFKA_SPLIT_ERROR,
                    format(
                            "No committed offset found for topic '%s' partition %s and group ID '%s'",
                            tableHandle.topicName(),
                            topicPartition.partition(),
                            groupId));
        }
        return new CommittedOffsetResolution(applyMissingOffsetPolicy(logStart, logEnd), true, Optional.of(missingOffsetPolicy));
    }

    private long applyMissingOffsetPolicy(long logStart, long logEnd)
    {
        return switch (missingOffsetPolicy) {
            case EARLIEST -> logStart;
            case LATEST -> logEnd;
            case ERROR -> throw new IllegalStateException("ERROR policy should be handled before applying it");
        };
    }

    private long clampCheckpointTarget(long logStart, long logEnd, long filteredEnd)
    {
        return min(logEnd, max(logStart, filteredEnd));
    }

    private record CommittedOffsetResolution(long committedBase, boolean missingOrInvalid, Optional<KafkaCommittedReadMissingOffsetPolicy> appliedPolicy)
    {
        private CommittedOffsetResolution
        {
            requireNonNull(appliedPolicy, "appliedPolicy is null");
        }
    }

    private record SplitPlanningResult(Optional<SplitRangeAndMetadata> split)
    {
        private SplitPlanningResult
        {
            requireNonNull(split, "split is null");
        }
    }

    private record SplitRangeAndMetadata(Range range, KafkaCommittedReadSplitMetadata metadata)
    {
        private SplitRangeAndMetadata
        {
            requireNonNull(range, "range is null");
            requireNonNull(metadata, "metadata is null");
        }
    }
}
