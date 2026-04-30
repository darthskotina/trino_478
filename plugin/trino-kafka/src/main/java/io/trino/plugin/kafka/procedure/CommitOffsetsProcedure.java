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
package io.trino.plugin.kafka.procedure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.trino.plugin.kafka.KafkaConfig;
import io.trino.plugin.kafka.KafkaConsumerFactory;
import io.trino.plugin.kafka.KafkaOffsetBoundsService;
import io.trino.plugin.kafka.KafkaSessionProperties;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeManager;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;

import java.lang.invoke.MethodHandle;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;

public class CommitOffsetsProcedure
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(CommitOffsetsProcedure.class);
    private static final MethodHandle COMMIT_OFFSETS;

    static {
        try {
            COMMIT_OFFSETS = lookup().unreflect(CommitOffsetsProcedure.class.getMethod(
                    "commitOffsets",
                    ConnectorSession.class,
                    ConnectorAccessControl.class,
                    String.class,
                    String.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Map.class,
                    Boolean.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final KafkaConsumerFactory consumerFactory;
    private final KafkaOffsetBoundsService offsetBoundsService;
    private final MapType offsetsArgumentType;
    private final Duration assignmentPollTimeout;
    private final Duration maxWaitForAssignment;
    private final Duration offsetCommitTimeout;

    @Inject
    public CommitOffsetsProcedure(
            KafkaConsumerFactory consumerFactory,
            KafkaOffsetBoundsService offsetBoundsService,
            TypeManager typeManager,
            KafkaConfig kafkaConfig)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory is null");
        this.offsetBoundsService = requireNonNull(offsetBoundsService, "offsetBoundsService is null");
        this.offsetsArgumentType = (MapType) requireNonNull(typeManager, "typeManager is null").getType(mapType(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        this.assignmentPollTimeout = Duration.ofMillis(kafkaConfig.getCommitOffsetsAssignmentPollTimeout().toMillis());
        this.maxWaitForAssignment = Duration.ofMillis(kafkaConfig.getCommitOffsetsAssignmentTimeout().toMillis());
        this.offsetCommitTimeout = Duration.ofMillis(kafkaConfig.getCommitOffsetsCommitTimeout().toMillis());
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "commit_offsets",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("GROUP_ID", VARCHAR, false, null),
                        new Procedure.Argument("PARTITION", BIGINT, false, null),
                        new Procedure.Argument("OFFSET", BIGINT, false, null),
                        new Procedure.Argument("OFFSETS", offsetsArgumentType, false, null),
                        new Procedure.Argument("ALLOW_OUT_OF_RANGE", BOOLEAN, false, false)),
                COMMIT_OFFSETS.bindTo(this));
    }

    public void commitOffsets(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String schemaName,
            String tableName,
            String groupId,
            Long partition,
            Long offset,
            Map<?, ?> offsets,
            Boolean allowOutOfRange)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doCommitOffsets(session, accessControl, schemaName, tableName, groupId, partition, offset, offsets, firstNonNull(allowOutOfRange, false));
        }
    }

    private void doCommitOffsets(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String schemaName,
            String tableName,
            String groupId,
            Long partition,
            Long offset,
            Map<?, ?> offsets,
            boolean allowOutOfRange)
    {
        checkProcedureArgument(schemaName != null && !schemaName.isBlank(), "schema_name cannot be null or blank");
        checkProcedureArgument(tableName != null && !tableName.isBlank(), "table_name cannot be null or blank");

        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        // Defense in depth for direct connector invocation; the engine also checks procedure execution.
        accessControl.checkCanExecuteProcedure(null, new SchemaRoutineName("system", "commit_offsets"));

        String resolvedGroupId = resolveGroupId(session, groupId);
        Map<Integer, Long> requested = getRequestedOffsets(partition, offset, offsets);

        KafkaTopicDescription topicDescription = offsetBoundsService.getTopicDescription(session, schemaTableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        String topicName = topicDescription.topicName();

        accessControl.checkCanSelectFromColumns(null, schemaTableName, ImmutableSet.of());

        validateBounds(session, topicName, requested, allowOutOfRange);
        commitOffsets(session, topicName, resolvedGroupId, requested);
    }

    private static String resolveGroupId(ConnectorSession session, String groupId)
    {
        String resolvedGroupId = Optional.ofNullable(groupId)
                .or(() -> KafkaSessionProperties.getConsumerGroupIdSessionProperty(session))
                .orElseThrow(() -> new TrinoException(
                        INVALID_PROCEDURE_ARGUMENT,
                        "group_id must be supplied either as a procedure argument or via session property 'consumer_group_id'"));
        checkProcedureArgument(!resolvedGroupId.isBlank(), "group_id cannot be blank");
        return resolvedGroupId;
    }

    private Map<Integer, Long> getRequestedOffsets(Long partition, Long offset, Map<?, ?> offsets)
    {
        if (offsets != null && (partition != null || offset != null)) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "must specify either partition+offset or offsets, not both");
        }
        if (offsets == null && partition == null && offset == null) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "must specify either partition+offset or offsets");
        }
        if (offsets == null && partition != null && offset == null) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "partition specified without offset; both must be supplied for the single-partition form");
        }
        if (offsets == null && partition == null) {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "offset specified without partition; both must be supplied for the single-partition form");
        }

        if (offsets == null) {
            return Map.of(validatePartition(partition), validateOffset(offset));
        }
        return getRequestedOffsets(offsets);
    }

    private static Map<Integer, Long> getRequestedOffsets(Map<?, ?> offsets)
    {
        checkProcedureArgument(!offsets.isEmpty(), "offsets map cannot be empty");
        ImmutableMap.Builder<Integer, Long> requested = ImmutableMap.builder();
        for (Map.Entry<?, ?> entry : offsets.entrySet()) {
            Object partition = entry.getKey();
            Object offset = entry.getValue();
            checkProcedureArgument(partition != null, "offsets map partition key cannot be null");
            checkProcedureArgument(offset != null, "offsets map offset value cannot be null");
            requested.put(
                    validatePartition((Long) partition),
                    validateOffset((Long) offset));
        }
        return requested.buildOrThrow();
    }

    private static int validatePartition(Long partition)
    {
        checkProcedureArgument(partition != null, "partition cannot be null");
        checkProcedureArgument(partition >= 0, "partition must not be negative");
        checkProcedureArgument(partition <= MAX_VALUE, "partition must be less than or equal to %s", MAX_VALUE);
        return partition.intValue();
    }

    private static long validateOffset(Long offset)
    {
        checkProcedureArgument(offset != null, "offset cannot be null");
        checkProcedureArgument(offset >= 0, "offset must not be negative");
        return offset;
    }

    private void validateBounds(ConnectorSession session, String topicName, Map<Integer, Long> requested, boolean allowOutOfRange)
    {
        if (allowOutOfRange) {
            offsetBoundsService.validatePartitionsExist(session, topicName, requested.keySet());
            return;
        }

        Map<TopicPartition, KafkaOffsetBoundsService.OffsetBounds> bounds = offsetBoundsService.getPartitionBounds(session, topicName, requested.keySet());
        requested.forEach((partition, offset) -> {
            KafkaOffsetBoundsService.OffsetBounds partitionBounds = bounds.get(new TopicPartition(topicName, partition));
            if (partitionBounds == null) {
                throw new TrinoException(
                        KAFKA_SPLIT_ERROR,
                        format("Partition %s does not exist for topic '%s'", partition, topicName));
            }
            if (offset < partitionBounds.logStart() || offset > partitionBounds.logEnd()) {
                throw new TrinoException(
                        INVALID_PROCEDURE_ARGUMENT,
                        format(
                                "Offset %s for partition %s is outside the broker range [%s, %s]; pass allow_out_of_range => true to override",
                                offset,
                                partition,
                                partitionBounds.logStart(),
                                partitionBounds.logEnd()));
            }
        });
    }

    private void commitOffsets(ConnectorSession session, String topicName, String groupId, Map<Integer, Long> requested)
    {
        KafkaConsumer<byte[], byte[]> consumer = null;
        Throwable failure = null;
        try {
            KafkaConsumer<byte[], byte[]> groupConsumer = consumerFactory.createForGroup(session, groupId);
            consumer = groupConsumer;
            groupConsumer.subscribe(Set.of(topicName), new ConsumerRebalanceListener()
            {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions)
                {
                    // Avoid fetch-position initialization before commit when deployments set
                    // auto.offset.reset=none and the group has no prior committed offsets.
                    groupConsumer.pause(partitions);
                }
            });
            Set<TopicPartition> requestedTopicPartitions = requested.keySet().stream()
                    .map(partition -> new TopicPartition(topicName, partition))
                    .collect(toImmutableSet());

            waitForAssignment(groupConsumer, topicName, groupId, requestedTopicPartitions);
            // Defensive no-op for already paused partitions assigned by the rebalance callback.
            groupConsumer.pause(groupConsumer.assignment());

            ImmutableMap.Builder<TopicPartition, OffsetAndMetadata> offsets = ImmutableMap.builder();
            requested.forEach((partition, offset) -> offsets.put(new TopicPartition(topicName, partition), new OffsetAndMetadata(offset)));
            try {
                groupConsumer.commitSync(offsets.buildOrThrow(), offsetCommitTimeout);
            }
            catch (TimeoutException e) {
                throw new TrinoException(
                        KAFKA_SPLIT_ERROR,
                        format("Timed out committing Kafka offsets for group ID '%s' on topic '%s' within %s", groupId, topicName, offsetCommitTimeout),
                        e);
            }
        }
        catch (KafkaException e) {
            failure = e;
            throw new TrinoException(KAFKA_SPLIT_ERROR, format("Failed to commit Kafka offsets for group ID '%s' on topic '%s'", groupId, topicName), e);
        }
        catch (RuntimeException e) {
            failure = e;
            throw e;
        }
        finally {
            closeConsumer(consumer, topicName, groupId, failure);
        }
    }

    private void waitForAssignment(KafkaConsumer<byte[], byte[]> consumer, String topicName, String groupId, Set<TopicPartition> requestedTopicPartitions)
    {
        Instant now = Instant.now();
        Instant deadline = now.plus(maxWaitForAssignment);
        while (Instant.now().isBefore(deadline)) {
            try {
                consumer.poll(assignmentPollTimeout);
            }
            catch (NoOffsetForPartitionException e) {
                consumer.pause(consumer.assignment());
            }
            if (consumer.assignment().containsAll(requestedTopicPartitions)) {
                return;
            }
        }

        Set<TopicPartition> missing = requestedTopicPartitions.stream()
                .filter(topicPartition -> !consumer.assignment().contains(topicPartition))
                .collect(toImmutableSet());
        throw new TrinoException(
                KAFKA_SPLIT_ERROR,
                format(
                        "Could not acquire partitions [%s] for group ID '%s' on topic '%s' within %ss; another consumer is likely a member of the same group",
                        missing.stream()
                                .map(topicPartition -> Integer.toString(topicPartition.partition()))
                                .collect(joining(", ")),
                        groupId,
                        topicName,
                        NANOSECONDS.toSeconds(maxWaitForAssignment.toNanos())));
    }

    private static void closeConsumer(KafkaConsumer<byte[], byte[]> consumer, String topicName, String groupId, Throwable failure)
    {
        if (consumer == null) {
            return;
        }
        RuntimeException closeFailure = null;
        try {
            consumer.unsubscribe();
        }
        catch (RuntimeException e) {
            if (failure != null) {
                failure.addSuppressed(e);
            }
            else {
                closeFailure = e;
            }
        }
        try {
            consumer.close();
        }
        catch (RuntimeException e) {
            if (failure != null) {
                failure.addSuppressed(e);
            }
            else {
                if (closeFailure != null) {
                    closeFailure.addSuppressed(e);
                }
                else {
                    closeFailure = e;
                }
            }
        }
        if (closeFailure != null) {
            log.warn(closeFailure, "Failed to close Kafka consumer after committing offsets for group ID '%s' on topic '%s'", groupId, topicName);
        }
    }

    private static <T> T firstNonNull(T value, T defaultValue)
    {
        return value == null ? defaultValue : value;
    }
}
