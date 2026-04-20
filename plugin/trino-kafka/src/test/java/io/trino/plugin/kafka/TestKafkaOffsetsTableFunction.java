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
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.stream.LongStream;

import static io.trino.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestKafkaOffsetsTableFunction
        extends AbstractTestQueryFramework
{
    private TestingKafka testingKafka;
    private String topicName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(TestingKafka.create());
        testingKafka.start();

        topicName = "offset_bounds_" + randomNameSuffix();

        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setExtraTopicDescription(ImmutableMap.of(
                        new SchemaTableName("default", topicName),
                        createEmptyTopicDescription(topicName, new SchemaTableName("default", topicName)).getValue()))
                .build();

        testingKafka.createTopicWithConfig(2, 1, topicName, false);
        testingKafka.sendMessages(LongStream.of(0, 2, 4, 6)
                .mapToObj(id -> new ProducerRecord<>(topicName, id, id)));

        return queryRunner;
    }

    @Test
    public void testOffsetsReturnsPartitionBounds()
    {
        String allPartitions = format(
                "SELECT partition_id, log_start_offset, log_end_offset, last_readable_offset " +
                        "FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s')) " +
                        "ORDER BY partition_id",
                topicName);
        assertQuery(
                allPartitions,
                "VALUES " +
                        "(CAST(0 AS BIGINT), CAST(0 AS BIGINT), CAST(4 AS BIGINT), CAST(3 AS BIGINT)), " +
                        "(CAST(1 AS BIGINT), CAST(0 AS BIGINT), CAST(0 AS BIGINT), CAST(NULL AS BIGINT))");

        String partitionZero = format(
                "SELECT partition_id, log_start_offset, log_end_offset, last_readable_offset " +
                        "FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s', partition => 0))",
                topicName);
        assertQuery(partitionZero, "VALUES (CAST(0 AS BIGINT), CAST(0 AS BIGINT), CAST(4 AS BIGINT), CAST(3 AS BIGINT))");
        assertQuery(
                format(
                        "SELECT log_end_offset, last_readable_offset FROM TABLE(system.offsets(schema_name => 'default', table_name => '%s', partition => 1))",
                        topicName),
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
                "SELECT * FROM TABLE(system.offsets(schema_name => 'default', table_name => '" + topicName + "', partition => -1))",
                "partition must not be negative");
        assertQueryFails(
                "SELECT * FROM TABLE(system.offsets(schema_name => 'default', table_name => '" + topicName + "', partition => 2147483648))",
                "partition must be less than or equal to 2147483647");
        assertQueryFails(
                "SELECT * FROM TABLE(system.offsets(schema_name => 'default', table_name => 'missing_offsets_table'))",
                ".*missing_offsets_table.*");
        assertQueryFails(
                "SELECT * FROM TABLE(system.offsets(schema_name => 'default', table_name => '" + topicName + "', partition => 5))",
                "Partition 5 does not exist for topic '" + topicName + "'");
    }
}
