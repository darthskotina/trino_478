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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKafkaSplitManager
{
    @Test
    public void testHasFilter()
    {
        TestingColumnHandle columnHandle = new TestingColumnHandle("partition_offset");

        assertThat(KafkaSplitManager.hasFilter(Constraint.alwaysTrue())).isFalse();
        assertThat(KafkaSplitManager.hasFilter(new Constraint(
                TupleDomain.all(),
                TRUE,
                Map.of(),
                bindings -> true,
                java.util.Set.of()))).isFalse();
        assertThat(KafkaSplitManager.hasFilter(new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(
                columnHandle,
                Domain.singleValue(BIGINT, 1L)))))).isTrue();
        assertThat(KafkaSplitManager.hasFilter(new Constraint(
                TupleDomain.all(),
                new Variable("filter", BOOLEAN),
                ImmutableMap.of("filter", columnHandle)))).isTrue();
        assertThat(KafkaSplitManager.hasFilter(new Constraint(
                TupleDomain.all(),
                TRUE,
                Map.of(),
                bindings -> true,
                java.util.Set.of(columnHandle)))).isTrue();
    }

    @Test
    public void testEstimateReadOffsets()
    {
        TopicPartition partitionZero = new TopicPartition("topic", 0);
        TopicPartition partitionOne = new TopicPartition("topic", 1);

        Map<TopicPartition, Long> beginOffsets = ImmutableMap.of(
                partitionZero, 5L,
                partitionOne, 10L);
        Map<TopicPartition, Long> endOffsets = ImmutableMap.of(
                partitionZero, 8L,
                partitionOne, 25L);

        assertThat(KafkaSplitManager.estimateReadOffsets(List.of(partitionZero, partitionOne), beginOffsets, endOffsets))
                .isEqualTo(18L);
    }

    @Test
    public void testGetSplitsRejectsReadWithoutFilter()
    {
        KafkaConfig kafkaConfig = new KafkaConfig()
                .setRequireFilter(true)
                .setMaxReadOffsets(0);
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, kafkaConfig);
        KafkaSplitManager splitManager = new KafkaSplitManager(
                new KafkaConsumerFactory()
                {
                    @Override
                    public KafkaConsumer<byte[], byte[]> create(io.trino.spi.connector.ConnectorSession session)
                    {
                        throw new AssertionError("Kafka consumer should not be created when the filter requirement fails");
                    }

                    @Override
                    public Properties configure(io.trino.spi.connector.ConnectorSession session)
                    {
                        return new Properties();
                    }
                },
                kafkaConfig,
                new KafkaFilterManager(session -> new Properties(), session -> new Properties(), internalFieldManager),
                internalFieldManager,
                new TestingContentSchemaProvider());

        assertThatThrownBy(() -> splitManager.getSplits(
                null,
                createSession(kafkaConfig, ImmutableMap.of()),
                createTableHandle(),
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("requires a WHERE clause");
    }

    @Test
    public void testGetSplitsRejectsReadAboveMaxOffsets()
    {
        KafkaConfig kafkaConfig = new KafkaConfig()
                .setRequireFilter(false)
                .setMaxReadOffsets(10);
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, kafkaConfig);
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        PartitionInfo partitionInfo = new PartitionInfo("topic", 0, new Node(1, "localhost", 9092), null, null);

        KafkaSplitManager splitManager = new KafkaSplitManager(
                new TestingKafkaConsumerFactory(
                        ImmutableMap.of("topic", List.of(partitionInfo)),
                        ImmutableMap.of(topicPartition, 0L),
                        ImmutableMap.of(topicPartition, 20L)),
                kafkaConfig,
                new KafkaFilterManager(session -> new Properties(), session -> new Properties(), internalFieldManager),
                internalFieldManager,
                new TestingContentSchemaProvider());

        assertThatThrownBy(() -> splitManager.getSplits(
                null,
                createSession(kafkaConfig, ImmutableMap.of()),
                createTableHandle(),
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("exceeds the configured limit of 10");
    }

    private static KafkaTableHandle createTableHandle()
    {
        return new KafkaTableHandle(
                "default",
                "topic",
                "topic",
                "raw",
                "raw",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                List.of(),
                TupleDomain.all());
    }

    private static TestingConnectorSession createSession(KafkaConfig kafkaConfig, Map<String, Object> propertyValues)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new KafkaSessionProperties(kafkaConfig).getSessionProperties())
                .setPropertyValues(propertyValues)
                .build();
    }

    private static class TestingContentSchemaProvider
            implements io.trino.plugin.kafka.schema.ContentSchemaProvider
    {
        @Override
        public Optional<String> getKey(KafkaTableHandle tableHandle)
        {
            return Optional.empty();
        }

        @Override
        public Optional<String> getMessage(KafkaTableHandle tableHandle)
        {
            return Optional.empty();
        }
    }

    private static class TestingKafkaConsumerFactory
            implements KafkaConsumerFactory
    {
        private final Map<String, List<PartitionInfo>> partitionsByTopic;
        private final Map<TopicPartition, Long> beginOffsets;
        private final Map<TopicPartition, Long> endOffsets;

        private TestingKafkaConsumerFactory(
                Map<String, List<PartitionInfo>> partitionsByTopic,
                Map<TopicPartition, Long> beginOffsets,
                Map<TopicPartition, Long> endOffsets)
        {
            this.partitionsByTopic = partitionsByTopic;
            this.beginOffsets = beginOffsets;
            this.endOffsets = endOffsets;
        }

        @Override
        public KafkaConsumer<byte[], byte[]> create(io.trino.spi.connector.ConnectorSession session)
        {
            return new TestingKafkaConsumer(partitionsByTopic, beginOffsets, endOffsets);
        }

        @Override
        public Properties configure(io.trino.spi.connector.ConnectorSession session)
        {
            return new Properties();
        }
    }

    private static class TestingKafkaConsumer
            extends KafkaConsumer<byte[], byte[]>
    {
        private final Map<String, List<PartitionInfo>> partitionsByTopic;
        private final Map<TopicPartition, Long> beginOffsets;
        private final Map<TopicPartition, Long> endOffsets;

        private TestingKafkaConsumer(
                Map<String, List<PartitionInfo>> partitionsByTopic,
                Map<TopicPartition, Long> beginOffsets,
                Map<TopicPartition, Long> endOffsets)
        {
            super(createConsumerProperties(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
            this.partitionsByTopic = partitionsByTopic;
            this.beginOffsets = beginOffsets;
            this.endOffsets = endOffsets;
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic)
        {
            return partitionsByTopic.getOrDefault(topic, List.of());
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions)
        {
            return beginOffsets;
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions)
        {
            return endOffsets;
        }

        @Override
        public void close()
        {
        }

        private static Properties createConsumerProperties()
        {
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "test-group");
            return properties;
        }
    }
}
