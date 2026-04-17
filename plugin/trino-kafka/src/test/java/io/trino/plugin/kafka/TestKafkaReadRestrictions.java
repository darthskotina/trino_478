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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.DockerClientFactory;

import java.util.UUID;
import java.util.stream.LongStream;

import static io.trino.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestKafkaReadRestrictions
        extends AbstractTestQueryFramework
{
    private static final int MESSAGE_COUNT = 20;

    private TestingKafka testingKafka;
    private String topicName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Assumptions.assumeTrue(isDockerAvailable(), "Docker is required for Kafka integration tests");

        testingKafka = closeAfterClass(TestingKafka.create());
        testingKafka.start();

        topicName = "test_read_restrictions_" + UUID.randomUUID().toString().replaceAll("-", "_");
        SchemaTableName schemaTableName = new SchemaTableName("default", topicName);

        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setExtraTopicDescription(ImmutableMap.of(schemaTableName, createEmptyTopicDescription(topicName, schemaTableName).getValue()))
                .addConnectorProperties(ImmutableMap.of(
                        "kafka.require-filter", "true",
                        "kafka.max-read-offsets", "10",
                        "kafka.messages-per-split", "100"))
                .build();

        testingKafka.createTopicWithConfig(1, 1, topicName, false);
        testingKafka.sendMessages(LongStream.range(0, MESSAGE_COUNT)
                .mapToObj(id -> new ProducerRecord<>(topicName, id, id)));

        return queryRunner;
    }

    private static boolean isDockerAvailable()
    {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Test
    public void testReadWithoutWhereIsRejected()
    {
        assertQueryFails(
                "SELECT count(*) FROM default." + topicName,
                ".*requires a WHERE clause.*");
    }

    @Test
    public void testReadAboveMaxOffsetsIsRejected()
    {
        assertQueryFails(
                "SELECT count(*) FROM default." + topicName + " WHERE _partition_offset < 20",
                ".*exceeds the configured limit of 10.*");
    }

    @Test
    public void testBoundedReadBelowMaxOffsetsSucceeds()
    {
        assertQuery(
                "SELECT count(*) FROM default." + topicName + " WHERE _partition_offset < 5",
                "VALUES BIGINT '5'");
    }

    @Test
    public void testSessionPropertiesCanRelaxRestrictions()
    {
        Session relaxedSession = Session.builder(getSession())
                .setSystemProperty("kafka.require_filter", "false")
                .setSystemProperty("kafka.max_read_offsets", Integer.toString(MESSAGE_COUNT))
                .build();

        assertQuery(
                relaxedSession,
                "SELECT count(*) FROM default." + topicName,
                "VALUES BIGINT '20'");
    }
}
