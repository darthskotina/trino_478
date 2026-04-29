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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.LongStream;

import static io.trino.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestKafkaCommitOffsetsProcedure
        extends AbstractTestQueryFramework
{
    private static final String DEFAULT_CATALOG = "kafka";
    private static final String EARLIEST_CATALOG = "kafka_earliest";
    private static final String LATEST_CATALOG = "kafka_latest";
    private static final String LEGACY_GROUP_ID = "legacy-default-group";

    private TestingKafka testingKafka;
    private String singlePartitionTopic;
    private String multiPartitionTopic;
    private String sessionGroupTopic;
    private String outOfRangeTopic;
    private String committedReadTopic;
    private String emptyTopic;
    private String accessControlTopic;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.create());
        testingKafka.start();

        singlePartitionTopic = newTopicName("commit_single");
        multiPartitionTopic = newTopicName("commit_multi");
        sessionGroupTopic = newTopicName("commit_session_group");
        outOfRangeTopic = newTopicName("commit_out_of_range");
        committedReadTopic = newTopicName("commit_then_read");
        emptyTopic = newTopicName("commit_empty");
        accessControlTopic = newTopicName("commit_access");

        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createEmptyTopicDescription(singlePartitionTopic, new SchemaTableName("default", singlePartitionTopic)))
                        .put(createEmptyTopicDescription(multiPartitionTopic, new SchemaTableName("default", multiPartitionTopic)))
                        .put(createEmptyTopicDescription(sessionGroupTopic, new SchemaTableName("default", sessionGroupTopic)))
                        .put(createEmptyTopicDescription(outOfRangeTopic, new SchemaTableName("default", outOfRangeTopic)))
                        .put(createEmptyTopicDescription(committedReadTopic, new SchemaTableName("default", committedReadTopic)))
                        .put(createEmptyTopicDescription(emptyTopic, new SchemaTableName("default", emptyTopic)))
                        .put(createEmptyTopicDescription(accessControlTopic, new SchemaTableName("default", accessControlTopic)))
                        .buildOrThrow())
                .addConnectorProperties(ImmutableMap.of(
                        "kafka.messages-per-split", "100",
                        "kafka.consumer-group-id", LEGACY_GROUP_ID))
                .build();

        createCatalog(queryRunner, EARLIEST_CATALOG, "EARLIEST");
        createCatalog(queryRunner, LATEST_CATALOG, "LATEST");

        testingKafka.createTopicWithConfig(1, 1, singlePartitionTopic, false);
        testingKafka.createTopicWithConfig(2, 1, multiPartitionTopic, false);
        testingKafka.createTopicWithConfig(1, 1, sessionGroupTopic, false);
        testingKafka.createTopicWithConfig(1, 1, outOfRangeTopic, false);
        testingKafka.createTopicWithConfig(1, 1, committedReadTopic, false);
        testingKafka.createTopicWithConfig(1, 1, emptyTopic, false);
        testingKafka.createTopicWithConfig(1, 1, accessControlTopic, false);

        sendMessages(singlePartitionTopic, 5);
        sendMessages(multiPartitionTopic, 6);
        sendMessages(sessionGroupTopic, 4);
        sendMessages(outOfRangeTopic, 3);
        sendMessages(committedReadTopic, 5);
        sendMessages(accessControlTopic, 1);

        return queryRunner;
    }

    @Test
    public void testCommitOffsetsSinglePartition()
    {
        String groupId = newGroupId();

        assertUpdate(format(
                "CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 5)",
                singlePartitionTopic,
                groupId));

        assertThat(getCommittedOffset(groupId, singlePartitionTopic)).hasValue(5L);
    }

    @Test
    public void testCommitOffsetsMultiPartition()
    {
        String groupId = newGroupId();
        Map<Integer, Long> endOffsets = getPartitionEndOffsets(multiPartitionTopic, 0, 1);

        assertUpdate(format(
                "CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', offsets => MAP(ARRAY[0, 1], ARRAY[%s, %s]))",
                multiPartitionTopic,
                groupId,
                endOffsets.get(0),
                endOffsets.get(1)));

        assertThat(getCommittedOffsets(groupId, multiPartitionTopic)).containsAllEntriesOf(endOffsets);
    }

    @Test
    public void testGroupIdSessionPropertyAndExplicitOverride()
    {
        String sessionGroupId = newGroupId();
        String explicitGroupId = newGroupId();
        Session session = Session.builder(getSession())
                .setCatalog(DEFAULT_CATALOG)
                .setSchema("default")
                .setCatalogSessionProperty(DEFAULT_CATALOG, "consumer_group_id", sessionGroupId)
                .build();

        assertUpdate(
                session,
                format("CALL system.commit_offsets(schema_name => 'default', table_name => '%s', partition => 0, offset => 2)", sessionGroupTopic));
        assertThat(getCommittedOffset(sessionGroupId, sessionGroupTopic)).hasValue(2L);

        assertUpdate(
                session,
                format(
                        "CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 3)",
                        sessionGroupTopic,
                        explicitGroupId));
        assertThat(getCommittedOffset(explicitGroupId, sessionGroupTopic)).hasValue(3L);
        assertThat(getCommittedOffset(sessionGroupId, sessionGroupTopic)).hasValue(2L);
    }

    @Test
    public void testValidationFailuresDoNotCommit()
    {
        String groupId = newGroupId();

        assertQueryFails(
                format("CALL system.commit_offsets(schema_name => 'default', table_name => '%s', partition => 0, offset => 1)", singlePartitionTopic),
                ".*group_id must be supplied.*consumer_group_id.*");
        assertQueryFails(
                format("CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '', partition => 0, offset => 1)", singlePartitionTopic),
                ".*group_id cannot be blank.*");
        assertQueryFails(
                format("CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0)", singlePartitionTopic, groupId),
                ".*partition specified without offset.*");
        assertQueryFails(
                format("CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', offset => 1)", singlePartitionTopic, groupId),
                ".*offset specified without partition.*");
        assertQueryFails(
                format("CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => -1)", singlePartitionTopic, groupId),
                ".*offset must not be negative.*");
        assertQueryFails(
                format("CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 99, offset => 0)", singlePartitionTopic, groupId),
                format(".*Partition 99 does not exist for topic '%s'.*", singlePartitionTopic));

        assertThat(getCommittedOffset(groupId, singlePartitionTopic)).isEmpty();
    }

    @Test
    public void testOutOfRangeValidationAndOverride()
    {
        String rejectedGroupId = newGroupId();
        String acceptedGroupId = newGroupId();

        assertQueryFails(
                format("CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 1000)", outOfRangeTopic, rejectedGroupId),
                ".*outside the broker range \\[0, 3\\].*");
        assertThat(getCommittedOffset(rejectedGroupId, outOfRangeTopic)).isEmpty();

        assertUpdate(format(
                "CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 1000, allow_out_of_range => true)",
                outOfRangeTopic,
                acceptedGroupId));
        assertThat(getCommittedOffset(acceptedGroupId, outOfRangeTopic)).hasValue(1000L);
    }

    @Test
    public void testCommittedReadPolicyAfterOutOfRangeOverride()
    {
        String errorGroupId = newGroupId();
        assertUpdate(format(
                "CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 1000, allow_out_of_range => true)",
                outOfRangeTopic,
                errorGroupId));
        assertQueryFails(
                committedReadSession(DEFAULT_CATALOG, errorGroupId),
                format("SELECT count(*) FROM default.%s", outOfRangeTopic),
                ".*outside the current broker range \\[0, 3\\].*");

        String earliestGroupId = newGroupId();
        assertUpdate(format(
                "CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 1000, allow_out_of_range => true)",
                outOfRangeTopic,
                earliestGroupId));
        assertThat(computeActual(committedReadSession(EARLIEST_CATALOG, earliestGroupId), format("SELECT count(*) FROM default.%s", outOfRangeTopic)).getOnlyValue()).isEqualTo(3L);

        String latestGroupId = newGroupId();
        assertUpdate(format(
                "CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 1000, allow_out_of_range => true)",
                outOfRangeTopic,
                latestGroupId));
        assertThat(computeActual(committedReadSession(LATEST_CATALOG, latestGroupId), format("SELECT count(*) FROM default.%s", outOfRangeTopic)).getOnlyValue()).isEqualTo(0L);
        assertThat(getCommittedOffset(latestGroupId, outOfRangeTopic)).hasValue(3L);
    }

    @Test
    public void testEmptyTopicAndBrandNewGroup()
    {
        String groupId = newGroupId();

        assertUpdate(format(
                "CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 0)",
                emptyTopic,
                groupId));

        assertThat(getCommittedOffset(groupId, emptyTopic)).hasValue(0L);
    }

    @Test
    public void testProcedureRunsWithCommittedReadEnabledAndNoSessionGroup()
    {
        String groupId = newGroupId();
        Session session = Session.builder(getSession())
                .setCatalog(DEFAULT_CATALOG)
                .setSchema("default")
                .setCatalogSessionProperty(DEFAULT_CATALOG, "committed_read_enabled", "true")
                .build();

        assertUpdate(
                session,
                format(
                        "CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 5)",
                        singlePartitionTopic,
                        groupId));

        assertThat(getCommittedOffset(groupId, singlePartitionTopic)).hasValue(5L);
    }

    @Test
    public void testAfterTheFactCommitThenCommittedRead()
    {
        String groupId = newGroupId();

        assertThat(computeActual(format("SELECT count(*) FROM default.%s", committedReadTopic)).getOnlyValue()).isEqualTo(5L);
        assertUpdate(format(
                "CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 5)",
                committedReadTopic,
                groupId));
        assertThat(computeActual(committedReadSession(EARLIEST_CATALOG, groupId), format("SELECT count(*) FROM default.%s", committedReadTopic)).getOnlyValue()).isEqualTo(0L);

        sendMessages(committedReadTopic, 2, 5);
        assertThat(computeActual(committedReadSession(EARLIEST_CATALOG, groupId), format("SELECT count(*) FROM default.%s", committedReadTopic)).getOnlyValue()).isEqualTo(2L);
        assertThat(getCommittedOffset(groupId, committedReadTopic)).hasValue(7L);
    }

    @Test
    public void testSelectAccessControl()
    {
        assertAccessDenied(
                format("CALL system.commit_offsets(schema_name => 'default', table_name => '%s', group_id => '%s', partition => 0, offset => 0)", accessControlTopic, newGroupId()),
                "Cannot select from columns .*",
                privilege(accessControlTopic, SELECT_COLUMN));
    }

    private void createCatalog(QueryRunner queryRunner, String catalogName, String missingOffsetPolicy)
    {
        queryRunner.createCatalog(catalogName, "kafka", ImmutableMap.of(
                "kafka.nodes", testingKafka.getConnectString(),
                "kafka.messages-per-split", "100",
                "kafka.table-description-supplier", "test",
                "kafka.consumer-group-id", LEGACY_GROUP_ID,
                "kafka.committed-read-missing-offset-policy", missingOffsetPolicy));
    }

    private Session committedReadSession(String catalog, String groupId)
    {
        return Session.builder(getSession())
                .setCatalog(catalog)
                .setSchema("default")
                .setCatalogSessionProperty(catalog, "committed_read_enabled", "true")
                .setCatalogSessionProperty(catalog, "consumer_group_id", groupId)
                .build();
    }

    private Optional<Long> getCommittedOffset(String groupId, String topicName)
    {
        return Optional.ofNullable(getCommittedOffsets(groupId, topicName).get(0));
    }

    private Map<Integer, Long> getCommittedOffsets(String groupId, String topicName)
    {
        try (Admin admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString()))) {
            return admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get()
                    .entrySet().stream()
                    .filter(entry -> entry.getKey().topic().equals(topicName))
                    .collect(toMap(
                            entry -> entry.getKey().partition(),
                            entry -> entry.getValue().offset()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Integer, Long> getPartitionEndOffsets(String topicName, int... partitions)
    {
        List<TopicPartition> topicPartitions = java.util.Arrays.stream(partitions)
                .mapToObj(partition -> new TopicPartition(topicName, partition))
                .toList();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties("end-offset-reader-" + newGroupId()))) {
            return consumer.endOffsets(topicPartitions).entrySet().stream()
                    .collect(toMap(entry -> entry.getKey().partition(), Map.Entry::getValue));
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

    private static String newGroupId()
    {
        return "group_" + UUID.randomUUID().toString().replace("-", "");
    }
}
