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

import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.plugin.kafka.KafkaInternalFieldManager.InternalFieldId;
import io.trino.plugin.kafka.schema.ContentSchemaProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.assertions.Assert;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.LongStream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKafkaCommittedReadSplitManager
{
    @Test
    public void testCommittedReadPlansOneDataSplitPerPartition()
            throws Exception
    {
        try (TestingKafka testingKafka = TestingKafka.create()) {
            testingKafka.start();
            String topicName = topicName("one_split");
            testingKafka.createTopicWithConfig(2, 1, topicName, false);
            testingKafka.sendMessages(LongStream.range(0, 20).mapToObj(id -> new ProducerRecord<>(topicName, id, id)));

            KafkaConfig config = baseConfig(testingKafka)
                    .setMessagesPerSplit(1)
                    .setCommittedReadMissingOffsetPolicy(KafkaCommittedReadMissingOffsetPolicy.EARLIEST);
            KafkaSplitManager splitManager = splitManager(config);

            List<KafkaSplit> splits = getSplits(splitManager, committedReadSession(config, "group-" + UUID.randomUUID()), tableHandle(topicName, TupleDomain.all()));

            assertThat(splits).hasSize(2);
            assertThat(splits)
                    .allSatisfy(split -> {
                        assertThat(split.getCommittedReadSplitMetadata()).isPresent();
                        assertThat(split.getCommittedReadSplitMetadata().orElseThrow().checkpointOnly()).isFalse();
                    });
        }
    }

    @Test
    public void testLatestPlanningUsesCheckpointOnlySplitWithClampedCommitTarget()
            throws Exception
    {
        try (TestingKafka testingKafka = TestingKafka.create()) {
            testingKafka.start();
            String topicName = topicName("checkpoint");
            testingKafka.createTopicWithConfig(1, 1, topicName, false);
            testingKafka.sendMessages(LongStream.range(0, 10).mapToObj(id -> new ProducerRecord<>(topicName, id, id)));

            KafkaConfig config = baseConfig(testingKafka)
                    .setMessagesPerSplit(1)
                    .setCommittedReadMissingOffsetPolicy(KafkaCommittedReadMissingOffsetPolicy.LATEST);
            KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);
            KafkaSplitManager splitManager = splitManager(config, internalFieldManager);

            TupleDomain<io.trino.spi.connector.ColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(
                    internalFieldManager.getFieldById(InternalFieldId.PARTITION_OFFSET_FIELD).getColumnHandle(false),
                    Domain.create(ValueSet.ofRanges(io.trino.spi.predicate.Range.lessThan(BIGINT, 3L)), false)));
            List<KafkaSplit> splits = getSplits(splitManager, committedReadSession(config, "group-" + UUID.randomUUID()), tableHandle(topicName, constraint));

            assertThat(splits).hasSize(1);
            KafkaSplit split = splits.get(0);
            assertThat(split.getMessagesRange().begin()).isEqualTo(3L);
            assertThat(split.getMessagesRange().end()).isEqualTo(3L);
            assertThat(split.getCommittedReadSplitMetadata()).isPresent();
            assertThat(split.getCommittedReadSplitMetadata().orElseThrow().checkpointOnly()).isTrue();
            assertThat(split.getCommittedReadSplitMetadata().orElseThrow().commitTarget()).isEqualTo(3L);
        }
    }

    @Test
    public void testLatestCheckpointOnlySplitClampsFilteredEndBelowLogStart()
            throws Exception
    {
        try (TestingKafka testingKafka = TestingKafka.create()) {
            testingKafka.start();
            String topicName = topicName("clamped_checkpoint");
            testingKafka.createTopicWithConfig(1, 1, topicName, false);
            testingKafka.sendMessages(LongStream.range(0, 10).mapToObj(id -> new ProducerRecord<>(topicName, id, id)));

            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            advanceLogStart(testingKafka, topicPartition, 7L);

            KafkaConfig config = baseConfig(testingKafka)
                    .setMessagesPerSplit(1)
                    .setCommittedReadMissingOffsetPolicy(KafkaCommittedReadMissingOffsetPolicy.LATEST);
            KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);
            KafkaSplitManager splitManager = splitManager(config, internalFieldManager);

            TupleDomain<io.trino.spi.connector.ColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(
                    internalFieldManager.getFieldById(InternalFieldId.PARTITION_OFFSET_FIELD).getColumnHandle(false),
                    Domain.create(ValueSet.ofRanges(io.trino.spi.predicate.Range.lessThan(BIGINT, 3L)), false)));
            List<KafkaSplit> splits = getSplits(splitManager, committedReadSession(config, "group-" + UUID.randomUUID()), tableHandle(topicName, constraint));

            assertThat(splits).hasSize(1);
            KafkaSplit split = splits.get(0);
            assertThat(split.getMessagesRange().begin()).isEqualTo(7L);
            assertThat(split.getMessagesRange().end()).isEqualTo(7L);
            assertThat(split.getCommittedReadSplitMetadata()).isPresent();
            assertThat(split.getCommittedReadSplitMetadata().orElseThrow().checkpointOnly()).isTrue();
            assertThat(split.getCommittedReadSplitMetadata().orElseThrow().commitTarget()).isEqualTo(7L);
        }
    }

    @Test
    public void testLatestCheckpointOnlySplitClampsInvalidCommittedOffset()
            throws Exception
    {
        try (TestingKafka testingKafka = TestingKafka.create()) {
            testingKafka.start();
            String topicName = topicName("invalid_committed_offset");
            testingKafka.createTopicWithConfig(1, 1, topicName, false);
            testingKafka.sendMessages(LongStream.range(0, 10).mapToObj(id -> new ProducerRecord<>(topicName, id, id)));

            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            String groupId = "group-" + UUID.randomUUID();
            setCommittedOffset(testingKafka, groupId, topicPartition, 2L);
            advanceLogStart(testingKafka, topicPartition, 7L);

            KafkaConfig config = baseConfig(testingKafka)
                    .setMessagesPerSplit(1)
                    .setCommittedReadMissingOffsetPolicy(KafkaCommittedReadMissingOffsetPolicy.LATEST);
            KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);
            KafkaSplitManager splitManager = splitManager(config, internalFieldManager);

            TupleDomain<io.trino.spi.connector.ColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(
                    internalFieldManager.getFieldById(InternalFieldId.PARTITION_OFFSET_FIELD).getColumnHandle(false),
                    Domain.create(ValueSet.ofRanges(io.trino.spi.predicate.Range.lessThan(BIGINT, 3L)), false)));
            List<KafkaSplit> splits = getSplits(splitManager, committedReadSession(config, groupId), tableHandle(topicName, constraint));

            assertThat(splits).hasSize(1);
            KafkaSplit split = splits.get(0);
            assertThat(split.getMessagesRange().begin()).isEqualTo(7L);
            assertThat(split.getMessagesRange().end()).isEqualTo(7L);
            assertThat(split.getCommittedReadSplitMetadata()).isPresent();
            assertThat(split.getCommittedReadSplitMetadata().orElseThrow().checkpointOnly()).isTrue();
            assertThat(split.getCommittedReadSplitMetadata().orElseThrow().commitTarget()).isEqualTo(7L);
        }
    }

    @Test
    public void testCommittedReadRejectsOffsetLowerBoundByDefault()
            throws Exception
    {
        try (TestingKafka testingKafka = TestingKafka.create()) {
            testingKafka.start();
            String topicName = topicName("reject_rewind");
            testingKafka.createTopicWithConfig(1, 1, topicName, false);
            testingKafka.sendMessages(LongStream.range(0, 10).mapToObj(id -> new ProducerRecord<>(topicName, id, id)));

            KafkaConfig config = baseConfig(testingKafka)
                    .setCommittedReadMissingOffsetPolicy(KafkaCommittedReadMissingOffsetPolicy.EARLIEST);
            KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);
            KafkaSplitManager splitManager = splitManager(config, internalFieldManager);

            assertThatThrownBy(() -> getSplits(
                    splitManager,
                    committedReadSession(config, "group-" + UUID.randomUUID()),
                    tableHandle(topicName, partitionOffsetConstraint(internalFieldManager, io.trino.spi.predicate.Range.greaterThanOrEqual(BIGINT, 3L)))))
                    .hasMessageContaining("does not allow lower-bound predicates on '_partition_offset'");
        }
    }

    private static KafkaConfig baseConfig(TestingKafka testingKafka)
    {
        return new KafkaConfig()
                .setNodes(java.util.Set.of(testingKafka.getConnectString()))
                .setTableDescriptionSupplier("test");
    }

    private static KafkaSplitManager splitManager(KafkaConfig config)
            throws Exception
    {
        return splitManager(config, new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config));
    }

    private static KafkaSplitManager splitManager(KafkaConfig config, KafkaInternalFieldManager internalFieldManager)
            throws Exception
    {
        DefaultKafkaConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(config);
        DefaultKafkaAdminFactory adminFactory = new DefaultKafkaAdminFactory(config);
        KafkaFilterManager filterManager = new KafkaFilterManager(consumerFactory, adminFactory, internalFieldManager);
        ContentSchemaProvider schemaProvider = new ContentSchemaProvider()
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
        };
        return new KafkaSplitManager(consumerFactory, adminFactory, config, filterManager, schemaProvider, new KafkaCommittedReadRegistry());
    }

    private static ConnectorSession committedReadSession(KafkaConfig config, String groupId)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new KafkaSessionProperties(config).getSessionProperties())
                .setPropertyValues(Map.of(
                        "committed_read_enabled", true,
                        "committed_read_group_id", groupId))
                .build();
    }

    private static KafkaTableHandle tableHandle(String topicName, TupleDomain<io.trino.spi.connector.ColumnHandle> constraint)
    {
        return new KafkaTableHandle(
                "default",
                topicName,
                topicName,
                DummyRowDecoder.NAME,
                DummyRowDecoder.NAME,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                List.of(),
                constraint);
    }

    private static List<KafkaSplit> getSplits(KafkaSplitManager splitManager, ConnectorSession session, KafkaTableHandle tableHandle)
    {
        List<KafkaSplit> splits = new ArrayList<>();
        try (ConnectorSplitSource splitSource = splitManager.getSplits(KafkaTransactionHandle.INSTANCE, session, tableHandle, DynamicFilter.EMPTY, Constraint.alwaysTrue())) {
            while (!splitSource.isFinished()) {
                splits.addAll(splitSource.getNextBatch(1000).join().getSplits().stream()
                        .map(KafkaSplit.class::cast)
                        .toList());
            }
        }
        return splits;
    }

    private static TupleDomain<io.trino.spi.connector.ColumnHandle> partitionOffsetConstraint(KafkaInternalFieldManager internalFieldManager, io.trino.spi.predicate.Range... ranges)
    {
        return TupleDomain.withColumnDomains(Map.of(
                internalFieldManager.getFieldById(InternalFieldId.PARTITION_OFFSET_FIELD).getColumnHandle(false),
                Domain.create(ValueSet.ofRanges(Arrays.asList(ranges)), false)));
    }

    private static String topicName(String prefix)
    {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "_");
    }

    private static void advanceLogStart(TestingKafka testingKafka, TopicPartition topicPartition, long logStart)
            throws Exception
    {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", testingKafka.getConnectString()))) {
            admin.deleteRecords(Map.of(topicPartition, RecordsToDelete.beforeOffset(logStart)))
                    .all()
                    .get();
        }

        Assert.assertEventually(() -> assertThat(getBeginningOffset(testingKafka, topicPartition)).isEqualTo(logStart));
    }

    private static long getBeginningOffset(TestingKafka testingKafka, TopicPartition topicPartition)
    {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties(testingKafka, "beginning-offset-reader-" + UUID.randomUUID()))) {
            return consumer.beginningOffsets(List.of(topicPartition)).get(topicPartition);
        }
    }

    private static void setCommittedOffset(TestingKafka testingKafka, String groupId, TopicPartition topicPartition, long committedOffset)
            throws Exception
    {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", testingKafka.getConnectString()))) {
            admin.alterConsumerGroupOffsets(groupId, Map.of(topicPartition, new OffsetAndMetadata(committedOffset)))
                    .all()
                    .get();
        }
    }

    private static Properties consumerProperties(TestingKafka testingKafka, String groupId)
    {
        Properties properties = new Properties();
        properties.putAll(Map.of(
                "bootstrap.servers", testingKafka.getConnectString(),
                "group.id", groupId,
                "key.deserializer", ByteArrayDeserializer.class.getName(),
                "value.deserializer", ByteArrayDeserializer.class.getName()));
        return properties;
    }
}
