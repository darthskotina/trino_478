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
import io.trino.Session;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.LongStream;

import static io.trino.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static java.lang.String.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestKafkaCommittedReadMode
        extends AbstractTestQueryFramework
{
    private static final String DEFAULT_CATALOG = "kafka";
    private static final String EARLIEST_CATALOG = "kafka_earliest";
    private static final String LATEST_CATALOG = "kafka_latest";
    private static final String LEGACY_GROUP_ID = "legacy-default-group";

    private TestingKafka testingKafka;
    private String defaultModeTopic;
    private String resumeTopic;
    private String missingGroupTopic;
    private String missingOffsetTopic;
    private String earlyCloseTopic;
    private String latestTopic;
    private String createTimeTopic;
    private String duplicateTopic;
    private String rewindTopic;
    private String rewindEqualityTopic;
    private String rewindGreaterThanTopic;
    private String rewindSparseTopic;
    private String rewindMissingOffsetTopic;
    private String rewindInvalidOffsetTopic;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.create());
        testingKafka.start();

        defaultModeTopic = newTopicName("default_mode");
        resumeTopic = newTopicName("resume");
        missingGroupTopic = newTopicName("missing_group");
        missingOffsetTopic = newTopicName("missing_offset");
        earlyCloseTopic = newTopicName("early_close");
        latestTopic = newTopicName("latest");
        createTimeTopic = newTopicName("create_time");
        duplicateTopic = newTopicName("duplicate");
        rewindTopic = newTopicName("rewind");
        rewindEqualityTopic = newTopicName("rewind_equal");
        rewindGreaterThanTopic = newTopicName("rewind_greater_than");
        rewindSparseTopic = newTopicName("rewind_sparse");
        rewindMissingOffsetTopic = newTopicName("rewind_missing");
        rewindInvalidOffsetTopic = newTopicName("rewind_invalid");

        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createEmptyTopicDescription(defaultModeTopic, new SchemaTableName("default", defaultModeTopic)))
                        .put(createEmptyTopicDescription(resumeTopic, new SchemaTableName("default", resumeTopic)))
                        .put(createEmptyTopicDescription(missingGroupTopic, new SchemaTableName("default", missingGroupTopic)))
                        .put(createEmptyTopicDescription(missingOffsetTopic, new SchemaTableName("default", missingOffsetTopic)))
                        .put(createEmptyTopicDescription(earlyCloseTopic, new SchemaTableName("default", earlyCloseTopic)))
                        .put(createEmptyTopicDescription(latestTopic, new SchemaTableName("default", latestTopic)))
                        .put(createEmptyTopicDescription(createTimeTopic, new SchemaTableName("default", createTimeTopic)))
                        .put(createEmptyTopicDescription(duplicateTopic, new SchemaTableName("default", duplicateTopic)))
                        .put(createEmptyTopicDescription(rewindTopic, new SchemaTableName("default", rewindTopic)))
                        .put(createEmptyTopicDescription(rewindEqualityTopic, new SchemaTableName("default", rewindEqualityTopic)))
                        .put(createEmptyTopicDescription(rewindGreaterThanTopic, new SchemaTableName("default", rewindGreaterThanTopic)))
                        .put(createEmptyTopicDescription(rewindSparseTopic, new SchemaTableName("default", rewindSparseTopic)))
                        .put(createEmptyTopicDescription(rewindMissingOffsetTopic, new SchemaTableName("default", rewindMissingOffsetTopic)))
                        .put(createEmptyTopicDescription(rewindInvalidOffsetTopic, new SchemaTableName("default", rewindInvalidOffsetTopic)))
                        .buildOrThrow())
                .addConnectorProperties(ImmutableMap.of(
                        "kafka.messages-per-split", "100",
                        "kafka.consumer-group-id", LEGACY_GROUP_ID))
                .build();

        createCatalog(queryRunner, EARLIEST_CATALOG, ImmutableMap.of(
                "kafka.nodes", testingKafka.getConnectString(),
                "kafka.messages-per-split", "100",
                "kafka.table-description-supplier", "test",
                "kafka.consumer-group-id", LEGACY_GROUP_ID,
                "kafka.committed-read-missing-offset-policy", "EARLIEST"));
        createCatalog(queryRunner, LATEST_CATALOG, ImmutableMap.of(
                "kafka.nodes", testingKafka.getConnectString(),
                "kafka.messages-per-split", "100",
                "kafka.table-description-supplier", "test",
                "kafka.consumer-group-id", LEGACY_GROUP_ID,
                "kafka.committed-read-missing-offset-policy", "LATEST"));

        testingKafka.createTopicWithConfig(1, 1, defaultModeTopic, false);
        testingKafka.createTopicWithConfig(1, 1, resumeTopic, false);
        testingKafka.createTopicWithConfig(1, 1, missingGroupTopic, false);
        testingKafka.createTopicWithConfig(1, 1, missingOffsetTopic, false);
        testingKafka.createTopicWithConfig(1, 1, earlyCloseTopic, false);
        testingKafka.createTopicWithConfig(1, 1, latestTopic, false);
        testingKafka.createTopicWithConfig(1, 1, createTimeTopic, false);
        testingKafka.createTopicWithConfig(1, 1, duplicateTopic, false);
        testingKafka.createTopicWithConfig(1, 1, rewindTopic, false);
        testingKafka.createTopicWithConfig(1, 1, rewindEqualityTopic, false);
        testingKafka.createTopicWithConfig(1, 1, rewindGreaterThanTopic, false);
        testingKafka.createTopicWithConfig(1, 1, rewindSparseTopic, false);
        testingKafka.createTopicWithConfig(1, 1, rewindMissingOffsetTopic, false);
        testingKafka.createTopicWithConfig(1, 1, rewindInvalidOffsetTopic, false);
        return queryRunner;
    }

    @Test
    public void testDefaultModeDoesNotCommitOffsets()
    {
        sendMessages(defaultModeTopic, 20);

        assertThat(computeActual(format("SELECT count(*) FROM default.%s", defaultModeTopic)).getOnlyValue()).isEqualTo(20L);
        assertThat(getCommittedOffset(LEGACY_GROUP_ID, defaultModeTopic)).isEmpty();
    }

    @Test
    public void testCommittedReadCommitsAndResumesFromCommittedOffsets()
    {
        sendMessages(resumeTopic, 10);

        String groupA = "group_a_" + UUID.randomUUID().toString().replace("-", "");
        Session groupASession = committedReadSession(EARLIEST_CATALOG, groupA);
        assertThat(computeActual(groupASession, format("SELECT count(*) FROM default.%s", resumeTopic)).getOnlyValue()).isEqualTo(10L);
        assertThat(consumerGroupExists(groupA)).isTrue();
        assertThat(getCommittedOffset(groupA, resumeTopic)).hasValue(10L);

        sendMessages(resumeTopic, 5, 10);

        assertThat(computeActual(groupASession, format("SELECT count(*) FROM default.%s", resumeTopic)).getOnlyValue()).isEqualTo(5L);
        assertThat(getCommittedOffset(groupA, resumeTopic)).hasValue(15L);
        assertThat(computeActual(groupASession, format("SELECT count(*) FROM default.%s", resumeTopic)).getOnlyValue()).isEqualTo(0L);
        assertThat(getCommittedOffset(groupA, resumeTopic)).hasValue(15L);

        String groupB = "group_b_" + UUID.randomUUID().toString().replace("-", "");
        Session groupBSession = committedReadSession(EARLIEST_CATALOG, groupB);
        assertThat(computeActual(groupBSession, format("SELECT count(*) FROM default.%s", resumeTopic)).getOnlyValue()).isEqualTo(15L);
        assertThat(consumerGroupExists(groupB)).isTrue();
        assertThat(getCommittedOffset(groupB, resumeTopic)).hasValue(15L);
    }

    @Test
    public void testCommittedReadSupportsLimitQueries()
    {
        sendMessages(earlyCloseTopic, 50_000);

        String groupId = "group_limit_" + UUID.randomUUID().toString().replace("-", "");
        Session session = committedReadSession(EARLIEST_CATALOG, groupId);

        assertThat(computeActual(session, format("SELECT _partition_offset FROM default.%s LIMIT 1", earlyCloseTopic)).getOnlyValue()).isEqualTo(0L);
    }

    @Test
    public void testLatestPolicyCreatesCheckpointWithoutReadingHistoricalRows()
    {
        sendMessages(latestTopic, 12);

        String groupId = "group_latest_" + UUID.randomUUID().toString().replace("-", "");
        Session session = committedReadSession(LATEST_CATALOG, groupId);

        assertThat(computeActual(session, format("SELECT count(*) FROM default.%s", latestTopic)).getOnlyValue()).isEqualTo(0L);
        assertThat(getCommittedOffset(groupId, latestTopic)).hasValue(getPartitionEndOffset(latestTopic));
    }

    @Test
    public void testMissingGroupIdFailsAndLegacyGroupIsNotUsedAsFallback()
    {
        sendMessages(missingGroupTopic, 3, 100);

        Session session = Session.builder(getSession())
                .setCatalog(DEFAULT_CATALOG)
                .setSchema("default")
                .setCatalogSessionProperty(DEFAULT_CATALOG, "committed_read_enabled", "true")
                .build();

        assertQueryFails(session, format("SELECT count(*) FROM default.%s", missingGroupTopic), ".*Committed-read mode requires session property 'committed_read_group_id' to be set.*");
        assertThat(getCommittedOffset(LEGACY_GROUP_ID, missingGroupTopic)).isEmpty();
    }

    @Test
    public void testMissingOffsetPolicyErrorFails()
    {
        sendMessages(missingOffsetTopic, 2, 1000);

        String groupId = "group_error_" + UUID.randomUUID().toString().replace("-", "");
        Session session = committedReadSession(DEFAULT_CATALOG, groupId);

        assertQueryFails(session, format("SELECT count(*) FROM default.%s", missingOffsetTopic), ".*No committed offset found.*");
        assertThat(getCommittedOffset(groupId, missingOffsetTopic)).isEmpty();
    }

    @Test
    public void testCommittedReadRejectsUnsupportedPredicates()
    {
        sendMessages(createTimeTopic, 5);

        String groupId = "group_predicates_" + UUID.randomUUID().toString().replace("-", "");
        Session session = committedReadSession(EARLIEST_CATALOG, groupId);

        assertQueryFails(session, format("SELECT count(*) FROM default.%s WHERE _partition_offset >= 2", createTimeTopic), ".*does not allow lower-bound predicates on '_partition_offset'.*");
        assertQueryFails(session, format("SELECT count(*) FROM default.%s WHERE _timestamp >= TIMESTAMP '2024-01-01 00:00:00.000'", createTimeTopic), ".*does not allow lower-bound predicates on '_timestamp'.*");
        assertQueryFails(session, format("SELECT count(*) FROM default.%s WHERE _timestamp < TIMESTAMP '2100-01-01 00:00:00.000'", createTimeTopic), ".*allows '_timestamp' upper bounds only for LogAppendTime topics.*");
    }

    @Test
    public void testRepeatedTrackedScanFailsFast()
    {
        sendMessages(duplicateTopic, 4);

        String groupId = "group_duplicate_" + UUID.randomUUID().toString().replace("-", "");
        Session session = committedReadSession(EARLIEST_CATALOG, groupId);

        assertQueryFails(
                session,
                format(
                        "SELECT count(*) FROM default.%s a CROSS JOIN default.%s b",
                        duplicateTopic,
                        duplicateTopic),
                ".*does not allow multiple tracked scans.*");
    }

    @Test
    public void testCommittedReadRewindMovesCommittedOffsetBackwardAndResumesFromRewoundOffset()
    {
        sendMessages(rewindTopic, 20);

        String groupId = "group_rewind_" + UUID.randomUUID().toString().replace("-", "");
        Session baseSession = committedReadSession(EARLIEST_CATALOG, groupId);
        Session rewindSession = committedReadRewindSession(EARLIEST_CATALOG, groupId);

        assertThat(computeActual(baseSession, format("SELECT count(*) FROM default.%s", rewindTopic)).getOnlyValue()).isEqualTo(20L);
        assertThat(getCommittedOffset(groupId, rewindTopic)).hasValue(20L);

        assertQueryFails(baseSession, format("SELECT count(*) FROM default.%s WHERE _partition_offset >= 5 AND _partition_offset < 8", rewindTopic), ".*does not allow lower-bound predicates on '_partition_offset'.*");

        assertThat(computeActual(rewindSession, format("SELECT count(*) FROM default.%s WHERE _partition_offset >= 5 AND _partition_offset < 8", rewindTopic)).getOnlyValue()).isEqualTo(3L);
        assertThat(getCommittedOffset(groupId, rewindTopic)).hasValue(8L);

        assertThat(computeActual(baseSession, format("SELECT count(*) FROM default.%s", rewindTopic)).getOnlyValue()).isEqualTo(12L);
        assertThat(getCommittedOffset(groupId, rewindTopic)).hasValue(20L);
    }

    @Test
    public void testCommittedReadRewindSupportsExactOffsetPredicate()
    {
        sendMessages(rewindEqualityTopic, 20);

        String groupId = "group_rewind_equal_" + UUID.randomUUID().toString().replace("-", "");
        Session baseSession = committedReadSession(EARLIEST_CATALOG, groupId);
        Session rewindSession = committedReadRewindSession(EARLIEST_CATALOG, groupId);

        assertThat(computeActual(baseSession, format("SELECT count(*) FROM default.%s", rewindEqualityTopic)).getOnlyValue()).isEqualTo(20L);
        assertThat(getCommittedOffset(groupId, rewindEqualityTopic)).hasValue(20L);

        assertThat(computeActual(rewindSession, format("SELECT count(*) FROM default.%s WHERE _partition_offset = 5", rewindEqualityTopic)).getOnlyValue()).isEqualTo(1L);
        assertThat(getCommittedOffset(groupId, rewindEqualityTopic)).hasValue(6L);

        assertThat(computeActual(baseSession, format("SELECT count(*) FROM default.%s", rewindEqualityTopic)).getOnlyValue()).isEqualTo(14L);
        assertThat(getCommittedOffset(groupId, rewindEqualityTopic)).hasValue(20L);
    }

    @Test
    public void testCommittedReadRewindSupportsGreaterThanOffsetPredicate()
    {
        sendMessages(rewindGreaterThanTopic, 20);

        String groupId = "group_rewind_gt_" + UUID.randomUUID().toString().replace("-", "");
        Session baseSession = committedReadSession(EARLIEST_CATALOG, groupId);
        Session rewindSession = committedReadRewindSession(EARLIEST_CATALOG, groupId);

        assertThat(computeActual(baseSession, format("SELECT count(*) FROM default.%s", rewindGreaterThanTopic)).getOnlyValue()).isEqualTo(20L);
        assertThat(getCommittedOffset(groupId, rewindGreaterThanTopic)).hasValue(20L);

        assertThat(computeActual(rewindSession, format("SELECT count(*) FROM default.%s WHERE _partition_offset > 5 AND _partition_offset < 8", rewindGreaterThanTopic)).getOnlyValue()).isEqualTo(2L);
        assertThat(getCommittedOffset(groupId, rewindGreaterThanTopic)).hasValue(8L);

        assertThat(computeActual(baseSession, format("SELECT count(*) FROM default.%s", rewindGreaterThanTopic)).getOnlyValue()).isEqualTo(12L);
        assertThat(getCommittedOffset(groupId, rewindGreaterThanTopic)).hasValue(20L);
    }

    @Test
    public void testCommittedReadRewindRejectsDiscontiguousOffsetPredicates()
    {
        sendMessages(rewindSparseTopic, 120);

        String groupId = "group_rewind_sparse_" + UUID.randomUUID().toString().replace("-", "");
        Session rewindSession = committedReadRewindSession(EARLIEST_CATALOG, groupId);

        assertQueryFails(
                rewindSession,
                format("SELECT count(*) FROM default.%s WHERE _partition_offset IN (5, 100)", rewindSparseTopic),
                ".*supports only contiguous '_partition_offset' window predicates.*");
        assertQueryFails(
                rewindSession,
                format("SELECT count(*) FROM default.%s WHERE _partition_offset < 5 OR (_partition_offset >= 10 AND _partition_offset < 20)", rewindSparseTopic),
                ".*supports only contiguous '_partition_offset' window predicates.*");
    }

    @Test
    public void testCommittedReadRewindReadsHistoricalWindowForLatestMissingOffsetPolicy()
    {
        sendMessages(rewindMissingOffsetTopic, 10);

        String groupId = "group_rewind_missing_" + UUID.randomUUID().toString().replace("-", "");
        Session session = committedReadRewindSession(LATEST_CATALOG, groupId);

        assertThat(computeActual(session, format("SELECT count(*) FROM default.%s WHERE _partition_offset >= 2 AND _partition_offset < 5", rewindMissingOffsetTopic)).getOnlyValue()).isEqualTo(3L);
        assertThat(getCommittedOffset(groupId, rewindMissingOffsetTopic)).hasValue(5L);
    }

    @Test
    public void testCommittedReadRewindReadsHistoricalWindowForLatestInvalidOffsetPolicy()
            throws Exception
    {
        sendMessages(rewindInvalidOffsetTopic, 10);

        String groupId = "group_rewind_invalid_" + UUID.randomUUID().toString().replace("-", "");
        TopicPartition topicPartition = new TopicPartition(rewindInvalidOffsetTopic, 0);
        setCommittedOffset(groupId, topicPartition, 2L);
        advanceLogStart(topicPartition, 7L);

        Session session = committedReadRewindSession(LATEST_CATALOG, groupId);

        assertThat(computeActual(session, format("SELECT count(*) FROM default.%s WHERE _partition_offset >= 5 AND _partition_offset < 9", rewindInvalidOffsetTopic)).getOnlyValue()).isEqualTo(2L);
        assertThat(getCommittedOffset(groupId, rewindInvalidOffsetTopic)).hasValue(9L);
    }

    private void createCatalog(QueryRunner queryRunner, String catalogName, Map<String, String> properties)
    {
        queryRunner.createCatalog(catalogName, "kafka", properties);
    }

    private Session committedReadSession(String catalog, String groupId)
    {
        return Session.builder(getSession())
                .setCatalog(catalog)
                .setSchema("default")
                .setCatalogSessionProperty(catalog, "committed_read_enabled", "true")
                .setCatalogSessionProperty(catalog, "committed_read_group_id", groupId)
                .build();
    }

    private Session committedReadRewindSession(String catalog, String groupId)
    {
        return Session.builder(getSession())
                .setCatalog(catalog)
                .setSchema("default")
                .setCatalogSessionProperty(catalog, "committed_read_enabled", "true")
                .setCatalogSessionProperty(catalog, "committed_read_group_id", groupId)
                .setCatalogSessionProperty(catalog, "committed_read_allow_offset_rewind", "true")
                .build();
    }

    private Optional<Long> getCommittedOffset(String groupId, String topicName)
    {
        try (Admin admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString()))) {
            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            OffsetAndMetadata metadata = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get()
                    .get(topicPartition);
            return Optional.ofNullable(metadata).map(OffsetAndMetadata::offset);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean consumerGroupExists(String groupId)
    {
        try (Admin admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString()))) {
            admin.describeConsumerGroups(java.util.List.of(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get();
            return true;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long getPartitionEndOffset(String topicName)
    {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties("end-offset-reader-" + topicName))) {
            return consumer.endOffsets(java.util.List.of(new TopicPartition(topicName, 0)))
                    .get(new TopicPartition(topicName, 0));
        }
    }

    private void advanceLogStart(TopicPartition topicPartition, long logStart)
            throws Exception
    {
        try (Admin admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString()))) {
            admin.deleteRecords(Map.of(topicPartition, RecordsToDelete.beforeOffset(logStart)))
                    .all()
                    .get();
        }

        assertThat(getPartitionBeginningOffset(topicPartition.topic())).isEqualTo(logStart);
    }

    private void setCommittedOffset(String groupId, TopicPartition topicPartition, long committedOffset)
            throws Exception
    {
        try (Admin admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString()))) {
            admin.alterConsumerGroupOffsets(groupId, Map.of(topicPartition, new OffsetAndMetadata(committedOffset)))
                    .all()
                    .get();
        }
    }

    private long getPartitionBeginningOffset(String topicName)
    {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties("begin-offset-reader-" + topicName))) {
            return consumer.beginningOffsets(java.util.List.of(new TopicPartition(topicName, 0)))
                    .get(new TopicPartition(topicName, 0));
        }
    }

    private Properties consumerProperties(String groupId)
    {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString());
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }

    private void sendMessages(String topicName, long count)
    {
        sendMessages(topicName, count, 0);
    }

    private void sendMessages(String topicName, long count, long startId)
    {
        testingKafka.sendMessages(LongStream.range(startId, startId + count)
                .mapToObj(id -> new ProducerRecord<>(topicName, id, id)));
    }

    private static String newTopicName(String prefix)
    {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "_");
    }
}
