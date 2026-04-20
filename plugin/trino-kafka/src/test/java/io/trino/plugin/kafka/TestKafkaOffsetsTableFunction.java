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
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.assertions.Assert;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.RecordsToDelete;
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
import java.util.stream.LongStream;

import static io.trino.plugin.kafka.util.TestUtils.createDescription;
import static io.trino.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static io.trino.plugin.kafka.util.TestUtils.createFieldGroup;
import static io.trino.plugin.kafka.util.TestUtils.createOneFieldDescription;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_FUNCTION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestKafkaOffsetsTableFunction
        extends AbstractTestQueryFramework
{
    private static final JsonCodec<KafkaOffsetBoundsFunctionHandle> FUNCTION_HANDLE_CODEC = JsonCodec.jsonCodec(KafkaOffsetBoundsFunctionHandle.class);
    private static final JsonCodec<KafkaOffsetBoundsSplit> SPLIT_CODEC = JsonCodec.jsonCodec(KafkaOffsetBoundsSplit.class);

    private TestingKafka testingKafka;
    private String offsetsTableName;
    private String retainedTableName;
    private String aliasedTableName;
    private String aliasedTopicName;
    private String metadataOnlyTableName;
    private String metadataOnlyTopicName;
    private String accessControlTableName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.create());
        testingKafka.start();

        offsetsTableName = "offset_bounds_" + randomNameSuffix();
        retainedTableName = "retained_offsets_" + randomNameSuffix();
        aliasedTableName = "orders_alias_" + randomNameSuffix();
        aliasedTopicName = "orders_topic_" + randomNameSuffix();
        metadataOnlyTableName = "metadata_only_" + randomNameSuffix();
        metadataOnlyTopicName = "metadata_topic_" + randomNameSuffix();
        accessControlTableName = "access_control_" + randomNameSuffix();

        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createEmptyTopicDescription(offsetsTableName, new SchemaTableName("default", offsetsTableName)))
                        .put(createEmptyTopicDescription(retainedTableName, new SchemaTableName("default", retainedTableName)))
                        .put(createEmptyTopicDescription(aliasedTopicName, new SchemaTableName("default", aliasedTableName)))
                        .put(
                                new SchemaTableName("default", metadataOnlyTableName),
                                createDescription(
                                        metadataOnlyTableName,
                                        "default",
                                        metadataOnlyTopicName,
                                        createFieldGroup("does_not_exist", ImmutableList.of())))
                        .put(
                                new SchemaTableName("default", accessControlTableName),
                                createDescription(
                                        accessControlTableName,
                                        "default",
                                        accessControlTableName,
                                        createFieldGroup("json", ImmutableList.of(createOneFieldDescription("value", BIGINT)))))
                        .buildOrThrow())
                .build();

        testingKafka.createTopicWithConfig(2, 1, offsetsTableName, false);
        testingKafka.createTopicWithConfig(1, 1, retainedTableName, false);
        testingKafka.createTopicWithConfig(1, 1, aliasedTopicName, false);
        testingKafka.createTopicWithConfig(1, 1, metadataOnlyTopicName, false);
        testingKafka.createTopicWithConfig(1, 1, accessControlTableName, false);

        testingKafka.sendMessages(LongStream.of(0, 2, 4, 6)
                .mapToObj(id -> new ProducerRecord<>(offsetsTableName, id, id)));
        testingKafka.sendMessages(LongStream.range(0, 5)
                .mapToObj(id -> new ProducerRecord<>(retainedTableName, id, id)));
        testingKafka.sendMessages(LongStream.of(11, 22, 33)
                .mapToObj(id -> new ProducerRecord<>(aliasedTopicName, id, id)));
        testingKafka.sendMessages(LongStream.of(1, 2)
                .mapToObj(id -> new ProducerRecord<>(metadataOnlyTopicName, id, id)));
        testingKafka.sendMessages(LongStream.of(7)
                .mapToObj(id -> new ProducerRecord<>(accessControlTableName, id, id)));

        return queryRunner;
    }

    @Test
    public void testOffsetsReturnsPartitionBounds()
    {
        assertQuery(
                format(
                        "SELECT partition_id, log_start_offset, log_end_offset, last_readable_offset " +
                                "FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s')) " +
                                "ORDER BY partition_id",
                        offsetsTableName),
                "VALUES " +
                        "(CAST(0 AS BIGINT), CAST(0 AS BIGINT), CAST(4 AS BIGINT), CAST(3 AS BIGINT)), " +
                        "(CAST(1 AS BIGINT), CAST(0 AS BIGINT), CAST(0 AS BIGINT), CAST(NULL AS BIGINT))");

        assertQuery(
                format(
                        "SELECT partition_id, log_start_offset, log_end_offset, last_readable_offset " +
                                "FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s', partition => 0))",
                        offsetsTableName),
                "VALUES (CAST(0 AS BIGINT), CAST(0 AS BIGINT), CAST(4 AS BIGINT), CAST(3 AS BIGINT))");
        assertQuery(
                format(
                        "SELECT log_end_offset, last_readable_offset " +
                                "FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s', partition => 1))",
                        offsetsTableName),
                "VALUES (CAST(0 AS BIGINT), CAST(NULL AS BIGINT))");
    }

    @Test
    public void testOffsetsValidation()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(system.offsets(schema_name => '', table_name => 'x'))",
                "schema_name cannot be blank");
        assertQueryFails(
                "SELECT * FROM TABLE(system.offsets(schema_name => 'default', table_name => '   '))",
                "table_name cannot be blank");
        assertQueryFails(
                format(
                        "SELECT * FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s', partition => -1))",
                        offsetsTableName),
                "partition must not be negative");
        assertQueryFails(
                format(
                        "SELECT * FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s', partition => 2147483648))",
                        offsetsTableName),
                "partition must be less than or equal to 2147483647");
        assertQueryFails(
                "SELECT * FROM TABLE(system.offsets(schema_name => 'default', table_name => 'missing_offsets_table'))",
                ".*missing_offsets_table.*");
        assertQueryFails(
                format(
                        "SELECT * FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s', partition => 5))",
                        offsetsTableName),
                format("Partition 5 does not exist for topic '%s'", offsetsTableName));
    }

    @Test
    public void testOffsetsSupportsDerivedAggregation()
    {
        assertQuery(
                format(
                        "SELECT min(log_start_offset), max(log_end_offset), max(last_readable_offset) " +
                                "FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s'))",
                        offsetsTableName),
                "VALUES (CAST(0 AS BIGINT), CAST(4 AS BIGINT), CAST(3 AS BIGINT))");
    }

    @Test
    public void testOffsetsResolvesAliasedTableNames()
    {
        assertQuery(
                format(
                        "SELECT partition_id, log_start_offset, log_end_offset, last_readable_offset " +
                                "FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s'))",
                        aliasedTableName),
                "VALUES (CAST(0 AS BIGINT), CAST(0 AS BIGINT), CAST(3 AS BIGINT), CAST(2 AS BIGINT))");
    }

    @Test
    public void testOffsetsReturnsAdvancedLogStartAfterRetention()
            throws Exception
    {
        advanceLogStart(new TopicPartition(retainedTableName, 0), 3);

        assertQuery(
                format(
                        "SELECT partition_id, log_start_offset, log_end_offset, last_readable_offset " +
                                "FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s'))",
                        retainedTableName),
                "VALUES (CAST(0 AS BIGINT), CAST(3 AS BIGINT), CAST(5 AS BIGINT), CAST(4 AS BIGINT))");
    }

    @Test
    public void testOffsetsUseMetadataOnlyRuntimePath()
    {
        assertQuery(
                format(
                        "SELECT partition_id, log_start_offset, log_end_offset, last_readable_offset " +
                                "FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s'))",
                        metadataOnlyTableName),
                "VALUES (CAST(0 AS BIGINT), CAST(0 AS BIGINT), CAST(2 AS BIGINT), CAST(1 AS BIGINT))");

        assertThatThrownBy(() -> computeActual("SELECT _partition_offset FROM default." + metadataOnlyTableName))
                .hasMessageContaining("does_not_exist");
    }

    @Test
    public void testOffsetsAccessControl()
    {
        String functionSql = format(
                "SELECT * FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s'))",
                accessControlTableName);

        assertAccessDenied(
                functionSql,
                "Cannot execute function .*",
                privilege("kafka.system.offsets", EXECUTE_FUNCTION));
        assertAccessDenied(
                functionSql,
                "Cannot select from columns .*",
                privilege(accessControlTableName, SELECT_COLUMN));
        assertAccessAllowed(
                functionSql,
                privilege(accessControlTableName + ".value", SELECT_COLUMN));
        assertAccessDenied(
                "SELECT value FROM default." + accessControlTableName,
                "Cannot select from columns .*",
                privilege(accessControlTableName + ".value", SELECT_COLUMN));
    }

    @Test
    public void testOffsetFunctionHandleAndSplitJsonRoundTrip()
    {
        KafkaOffsetBoundsFunctionHandle expectedHandle = new KafkaOffsetBoundsFunctionHandle(
                new SchemaTableName("default", aliasedTableName),
                aliasedTopicName,
                Optional.of(0));
        assertThat(FUNCTION_HANDLE_CODEC.fromJson(FUNCTION_HANDLE_CODEC.toJson(expectedHandle))).isEqualTo(expectedHandle);

        KafkaOffsetBoundsSplit expectedSplit = new KafkaOffsetBoundsSplit(1, 2, 5);
        KafkaOffsetBoundsSplit actualSplit = SPLIT_CODEC.fromJson(SPLIT_CODEC.toJson(expectedSplit));
        assertThat(actualSplit.getPartitionId()).isEqualTo(expectedSplit.getPartitionId());
        assertThat(actualSplit.getLogStartOffset()).isEqualTo(expectedSplit.getLogStartOffset());
        assertThat(actualSplit.getLogEndOffset()).isEqualTo(expectedSplit.getLogEndOffset());
    }

    private void advanceLogStart(TopicPartition topicPartition, long logStart)
            throws Exception
    {
        try (Admin admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString()))) {
            admin.deleteRecords(Map.of(topicPartition, RecordsToDelete.beforeOffset(logStart)))
                    .all()
                    .get();
        }

        Assert.assertEventually(() -> assertThat(getBeginningOffset(topicPartition)).isEqualTo(logStart));
    }

    private long getBeginningOffset(TopicPartition topicPartition)
    {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties("beginning-offset-reader-" + randomNameSuffix()))) {
            return consumer.beginningOffsets(List.of(topicPartition)).get(topicPartition);
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
}
