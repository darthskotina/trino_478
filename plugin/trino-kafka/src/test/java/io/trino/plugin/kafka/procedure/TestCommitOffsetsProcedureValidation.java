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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.kafka.KafkaConfig;
import io.trino.plugin.kafka.KafkaConsumerFactory;
import io.trino.plugin.kafka.KafkaOffsetBoundsService;
import io.trino.plugin.kafka.KafkaSessionProperties;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.plugin.kafka.schema.MapBasedTableDescriptionSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.AccessDeniedException;
import io.trino.testing.TestingConnectorSession;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denySelectColumns;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCommitOffsetsProcedureValidation
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("default", "orders");
    private static final String TOPIC_NAME = "orders-topic";

    @Test
    public void testRequiredNames()
            throws Exception
    {
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), null, "orders", "group", 0L, 0L, null, false), "schema_name cannot be null or blank");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "   ", "orders", "group", 0L, 0L, null, false), "schema_name cannot be null or blank");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", null, "group", 0L, 0L, null, false), "table_name cannot be null or blank");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "   ", "group", 0L, 0L, null, false), "table_name cannot be null or blank");
    }

    @Test
    public void testArgumentFormValidation()
            throws Exception
    {
        Map<Long, Long> offsets = Map.of(0L, 0L);

        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", null, null, null, false), "must specify either partition+offset or offsets");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, null, null, false), "partition specified without offset");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", null, 0L, null, false), "offset specified without partition");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, 0L, offsets, false), "must specify either partition+offset or offsets, not both");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, null, offsets, false), "must specify either partition+offset or offsets, not both");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", null, 0L, offsets, false), "must specify either partition+offset or offsets, not both");
    }

    @Test
    public void testSinglePartitionValidation()
            throws Exception
    {
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", -1L, 0L, null, false), "partition must not be negative");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", (long) Integer.MAX_VALUE + 1, 0L, null, false), "partition must be less than or equal to 2147483647");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, -1L, null, false), "offset must not be negative");
    }

    @Test
    public void testMapValidation()
            throws Exception
    {
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", null, null, Map.of(), false), "offsets map cannot be empty");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", null, null, mapWithNullKey(), false), "offsets map partition key cannot be null");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", null, null, mapWithNullValue(), false), "offsets map offset value cannot be null");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", null, null, Map.of(-1L, 0L), false), "partition must not be negative");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", null, null, Map.of((long) Integer.MAX_VALUE + 1, 0L), false), "partition must be less than or equal to 2147483647");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", null, null, Map.of(0L, -1L), false), "offset must not be negative");
    }

    @Test
    public void testGroupResolution()
            throws Exception
    {
        CapturingConsumerFactory factory = new CapturingConsumerFactory();
        assertThatThrownBy(() -> newProcedure(factory).commitOffsets(session(), allowAccess(), "default", "orders", null, 0L, 0L, null, false))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("group_id must be supplied either as a procedure argument or via session property 'consumer_group_id'")
                .extracting(throwable -> ((TrinoException) throwable).getErrorCode())
                .isEqualTo(INVALID_PROCEDURE_ARGUMENT.toErrorCode());

        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "   ", 0L, 0L, null, false), "group_id cannot be blank");

        assertThatThrownBy(() -> newProcedure(factory).commitOffsets(session("session-group", false), allowAccess(), "default", "orders", null, 0L, 5L, null, false))
                .isInstanceOf(ReachedCommitPathException.class);
        assertThat(factory.groupId).isEqualTo("session-group");

        assertThatThrownBy(() -> newProcedure(factory).commitOffsets(session("session-group", false), allowAccess(), "default", "orders", "explicit-group", 0L, 5L, null, false))
                .isInstanceOf(ReachedCommitPathException.class);
        assertThat(factory.groupId).isEqualTo("explicit-group");

        assertThatThrownBy(() -> newProcedure(factory).commitOffsets(session(null, true), allowAccess(), "default", "orders", "explicit-group", 0L, 5L, null, false))
                .isInstanceOf(ReachedCommitPathException.class);
        assertThat(factory.groupId).isEqualTo("explicit-group");
    }

    @Test
    public void testCommittedReadStillRequiresSessionGroupForReadPath()
    {
        assertThatThrownBy(() -> new CapturingConsumerFactory().configure(session(null, true)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("consumer_group_id")
                .extracting(throwable -> ((TrinoException) throwable).getErrorCode())
                .isEqualTo(INVALID_SESSION_PROPERTY.toErrorCode());
    }

    @Test
    public void testTableNotFound()
            throws Exception
    {
        TestingOffsetBoundsService boundsService = new TestingOffsetBoundsService();
        boundsService.topicDescription = Optional.empty();

        assertThatThrownBy(() -> newProcedure(new CapturingConsumerFactory(), boundsService).commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, 0L, null, false))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessageContaining("orders");
    }

    @Test
    public void testExecuteAccessControlRunsBeforeTableLookup()
            throws Exception
    {
        TestingOffsetBoundsService boundsService = new TestingOffsetBoundsService();
        boundsService.topicDescription = Optional.empty();

        assertThatThrownBy(() -> newProcedure(new CapturingConsumerFactory(), boundsService).commitOffsets(session(), denyExecuteAccess(), "default", "orders", "group", 0L, 0L, null, false))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Cannot execute procedure system.commit_offsets");
        assertThat(boundsService.topicDescriptionConsulted).isFalse();
    }

    @Test
    public void testSelectAccessControlRunsBeforeCommit()
            throws Exception
    {
        CapturingConsumerFactory consumerFactory = new CapturingConsumerFactory();

        assertThatThrownBy(() -> newProcedure(consumerFactory).commitOffsets(session(), denySelectAccess(), "default", "orders", "group", 0L, 0L, null, false))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Cannot select from columns");
        assertThat(consumerFactory.groupId).isNull();
    }

    @Test
    public void testBoundsValidation()
            throws Exception
    {
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, 4L, null, false), "outside the broker range [5, 10]; pass allow_out_of_range => true to override");
        assertInvalidProcedure(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, 11L, null, false), "outside the broker range [5, 10]; pass allow_out_of_range => true to override");

        assertThatThrownBy(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, 5L, null, false))
                .isInstanceOf(ReachedCommitPathException.class);
        assertThatThrownBy(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, 10L, null, false))
                .isInstanceOf(ReachedCommitPathException.class);

        TestingOffsetBoundsService boundsService = new TestingOffsetBoundsService();
        assertThatThrownBy(() -> newProcedure(new CapturingConsumerFactory(), boundsService).commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, 11L, null, true))
                .isInstanceOf(ReachedCommitPathException.class);
        assertThat(boundsService.partitionMetadataConsulted).isTrue();
        assertThat(boundsService.boundsConsulted).isFalse();
    }

    @Test
    public void testMissingPartition()
            throws Exception
    {
        assertKafkaSplitError(() -> newProcedure().commitOffsets(session(), allowAccess(), "default", "orders", "group", 99L, 0L, null, false), "Partition 99 does not exist for topic 'orders-topic'");

        TestingOffsetBoundsService incompleteBoundsService = new TestingOffsetBoundsService();
        incompleteBoundsService.returnIncompleteBounds = true;
        assertKafkaSplitError(
                () -> newProcedure(new CapturingConsumerFactory(), incompleteBoundsService).commitOffsets(session(), allowAccess(), "default", "orders", "group", 0L, 0L, null, false),
                "Partition 0 does not exist for topic 'orders-topic'");

        TestingOffsetBoundsService overrideBoundsService = new TestingOffsetBoundsService();
        assertKafkaSplitError(
                () -> newProcedure(new CapturingConsumerFactory(), overrideBoundsService).commitOffsets(session(), allowAccess(), "default", "orders", "group", 99L, 0L, null, true),
                "Partition 99 does not exist for topic 'orders-topic'");
        assertThat(overrideBoundsService.partitionMetadataConsulted).isTrue();
        assertThat(overrideBoundsService.partitionMetadataTopicName).isEqualTo(TOPIC_NAME);
        assertThat(overrideBoundsService.partitionMetadataRequestedPartitions).containsExactly(99);
    }

    private static CommitOffsetsProcedure newProcedure()
            throws Exception
    {
        return newProcedure(new CapturingConsumerFactory());
    }

    private static CommitOffsetsProcedure newProcedure(CapturingConsumerFactory consumerFactory)
            throws Exception
    {
        return newProcedure(consumerFactory, new TestingOffsetBoundsService());
    }

    private static CommitOffsetsProcedure newProcedure(CapturingConsumerFactory consumerFactory, KafkaOffsetBoundsService offsetBoundsService)
    {
        return new CommitOffsetsProcedure(consumerFactory, offsetBoundsService, TESTING_TYPE_MANAGER);
    }

    private static ConnectorSession session()
    {
        return session(null, false);
    }

    private static ConnectorSession session(String consumerGroupId, boolean committedReadEnabled)
    {
        Map<String, Object> propertyValues = new LinkedHashMap<>();
        propertyValues.put("committed_read_enabled", committedReadEnabled);
        if (consumerGroupId != null) {
            propertyValues.put("consumer_group_id", consumerGroupId);
        }
        KafkaConfig config = new KafkaConfig()
                .setNodes(Set.of("localhost:9092"));
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new KafkaSessionProperties(config).getSessionProperties())
                .setPropertyValues(propertyValues)
                .build();
    }

    private static ConnectorAccessControl allowAccess()
    {
        return new ConnectorAccessControl()
        {
            @Override
            public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure) {}

            @Override
            public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames) {}
        };
    }

    private static ConnectorAccessControl denyExecuteAccess()
    {
        return new ConnectorAccessControl()
        {
            @Override
            public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure)
            {
                denyExecuteProcedure(procedure.toString());
            }

            @Override
            public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames) {}
        };
    }

    private static ConnectorAccessControl denySelectAccess()
    {
        return new ConnectorAccessControl()
        {
            @Override
            public void checkCanExecuteProcedure(ConnectorSecurityContext context, SchemaRoutineName procedure) {}

            @Override
            public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
            {
                denySelectColumns(tableName.toString(), columnNames);
            }
        };
    }

    private static Map<Long, Long> mapWithNullKey()
    {
        Map<Long, Long> map = new LinkedHashMap<>();
        map.put(null, 0L);
        return map;
    }

    private static Map<Long, Long> mapWithNullValue()
    {
        Map<Long, Long> map = new LinkedHashMap<>();
        map.put(0L, null);
        return map;
    }

    private static void assertInvalidProcedure(ThrowingRunnable runnable, String message)
    {
        assertThatThrownBy(runnable::run)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining(message)
                .extracting(throwable -> ((TrinoException) throwable).getErrorCode())
                .isEqualTo(INVALID_PROCEDURE_ARGUMENT.toErrorCode());
    }

    private static void assertKafkaSplitError(ThrowingRunnable runnable, String message)
    {
        assertThatThrownBy(runnable::run)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining(message)
                .extracting(throwable -> ((TrinoException) throwable).getErrorCode())
                .isEqualTo(KAFKA_SPLIT_ERROR.toErrorCode());
    }

    private interface ThrowingRunnable
    {
        void run()
                throws Exception;
    }

    private static class CapturingConsumerFactory
            implements KafkaConsumerFactory
    {
        private String groupId;

        @Override
        public Properties baseProperties(ConnectorSession session)
        {
            return new Properties();
        }

        @Override
        public String resolveReadPathGroupId(ConnectorSession session)
        {
            if (KafkaSessionProperties.isCommittedReadEnabled(session)) {
                return KafkaSessionProperties.getRequiredCommittedReadGroupId(session);
            }
            return KafkaSessionProperties.getConsumerGroupIdSessionProperty(session)
                    .orElse("default-group");
        }

        @Override
        public KafkaConsumer<byte[], byte[]> createForGroup(ConnectorSession session, String groupId)
        {
            this.groupId = groupId;
            throw new ReachedCommitPathException();
        }
    }

    private static class TestingOffsetBoundsService
            extends KafkaOffsetBoundsService
    {
        private Optional<KafkaTopicDescription> topicDescription = Optional.of(new KafkaTopicDescription(
                TABLE_NAME.getTableName(),
                Optional.of(TABLE_NAME.getSchemaName()),
                TOPIC_NAME,
                Optional.empty(),
                Optional.empty()));
        private boolean topicDescriptionConsulted;
        private boolean boundsConsulted;
        private boolean partitionMetadataConsulted;
        private String partitionMetadataTopicName;
        private Set<Integer> partitionMetadataRequestedPartitions;
        private boolean returnIncompleteBounds;

        private TestingOffsetBoundsService()
                throws Exception
        {
            super(new CapturingConsumerFactory(), new MapBasedTableDescriptionSupplier(Map.of()));
        }

        @Override
        public Optional<KafkaTopicDescription> getTopicDescription(ConnectorSession session, SchemaTableName schemaTableName)
        {
            topicDescriptionConsulted = true;
            return topicDescription;
        }

        @Override
        public Map<TopicPartition, OffsetBounds> getPartitionBounds(ConnectorSession session, String topicName, Set<Integer> requestedPartitions)
        {
            boundsConsulted = true;
            if (returnIncompleteBounds) {
                return Map.of();
            }
            ImmutableMap.Builder<TopicPartition, OffsetBounds> bounds = ImmutableMap.builder();
            for (int partition : requestedPartitions) {
                if (partition != 0) {
                    throw new TrinoException(KAFKA_SPLIT_ERROR, "Partition %s does not exist for topic '%s'".formatted(partition, topicName));
                }
                bounds.put(new TopicPartition(topicName, partition), new OffsetBounds(5, 10));
            }
            return bounds.buildOrThrow();
        }

        @Override
        public void validatePartitionsExist(ConnectorSession session, String topicName, Set<Integer> requestedPartitions)
        {
            partitionMetadataConsulted = true;
            partitionMetadataTopicName = topicName;
            partitionMetadataRequestedPartitions = ImmutableSet.copyOf(requestedPartitions);
            for (int partition : requestedPartitions) {
                if (partition != 0) {
                    throw new TrinoException(KAFKA_SPLIT_ERROR, "Partition %s does not exist for topic '%s'".formatted(partition, topicName));
                }
            }
        }
    }

    private static class ReachedCommitPathException
            extends RuntimeException
    {
    }
}
