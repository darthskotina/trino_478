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
import io.trino.plugin.kafka.schema.MapBasedTableDescriptionSupplier;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.kafka.TestingKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.LongStream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKafkaScopedReadSplitManager
{
    private static final String VALIDATION_ONLY_TOPIC = "scoped_read_validation_only";

    @Test
    public void testRejectsUnconstrainedNormalModeScan()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(config, internalFieldManager, TupleDomain.all());
    }

    @Test
    public void testRejectsPartitionOnlyScope()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(config, internalFieldManager, partitionConstraint(internalFieldManager, io.trino.spi.predicate.Range.equal(BIGINT, 1L)));
    }

    @Test
    public void testRejectsOffsetOnlyScope()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(config, internalFieldManager, partitionOffsetConstraint(internalFieldManager, io.trino.spi.predicate.Range.greaterThanOrEqual(BIGINT, 1L)));
    }

    @Test
    public void testRejectsTimestampOnlyScope()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(config, internalFieldManager, timestampConstraint(internalFieldManager, io.trino.spi.predicate.Range.greaterThanOrEqual(TIMESTAMP_MILLIS, 1_000_000L)));
    }

    @Test
    public void testRejectsPartitionAndUpperBoundOffsetOnly()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(
                config,
                internalFieldManager,
                constraint(
                        internalFieldManager,
                        Optional.of(domain(io.trino.spi.predicate.Range.equal(BIGINT, 1L))),
                        Optional.of(domain(io.trino.spi.predicate.Range.lessThan(BIGINT, 5L))),
                        Optional.empty()));
    }

    @Test
    public void testRejectsPartitionAndUpperBoundTimestampOnly()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(
                config,
                internalFieldManager,
                constraint(
                        internalFieldManager,
                        Optional.of(domain(io.trino.spi.predicate.Range.equal(BIGINT, 1L))),
                        Optional.empty(),
                        Optional.of(domain(io.trino.spi.predicate.Range.lessThan(TIMESTAMP_MILLIS, 1_000_000L)))));
    }

    @Test
    public void testRejectsTrivialPartitionPredicate()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(
                config,
                internalFieldManager,
                constraint(
                        internalFieldManager,
                        Optional.of(domain(io.trino.spi.predicate.Range.greaterThan(BIGINT, -1L))),
                        Optional.of(domain(io.trino.spi.predicate.Range.greaterThanOrEqual(BIGINT, 1L))),
                        Optional.empty()));
    }

    @Test
    public void testRejectsPartitionRangePredicate()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(
                config,
                internalFieldManager,
                constraint(
                        internalFieldManager,
                        Optional.of(domain(io.trino.spi.predicate.Range.range(BIGINT, 0L, true, 1L, true))),
                        Optional.of(domain(io.trino.spi.predicate.Range.greaterThanOrEqual(BIGINT, 1L))),
                        Optional.empty()));
    }

    @Test
    public void testRejectsPartitionOffsetNotEqualPredicate()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(
                config,
                internalFieldManager,
                constraint(
                        internalFieldManager,
                        Optional.of(domain(io.trino.spi.predicate.Range.equal(BIGINT, 1L))),
                        Optional.of(domain(
                                io.trino.spi.predicate.Range.lessThan(BIGINT, 5L),
                                io.trino.spi.predicate.Range.greaterThan(BIGINT, 5L))),
                        Optional.empty()));
    }

    @Test
    public void testRejectsTrivialOffsetLowerBoundAtZero()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(
                config,
                internalFieldManager,
                constraint(
                        internalFieldManager,
                        Optional.of(domain(io.trino.spi.predicate.Range.equal(BIGINT, 1L))),
                        Optional.of(domain(io.trino.spi.predicate.Range.greaterThanOrEqual(BIGINT, 0L))),
                        Optional.empty()));
    }

    @Test
    public void testRejectsPartitionIsNotNullPredicate()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);

        assertScopeRejectedWithoutBroker(
                config,
                internalFieldManager,
                constraint(
                        internalFieldManager,
                        Optional.of(Domain.create(ValueSet.all(BIGINT), false)),
                        Optional.of(domain(io.trino.spi.predicate.Range.greaterThanOrEqual(BIGINT, 100L))),
                        Optional.empty()));
    }

    @Test
    public void testAcceptsExplicitPartitionAndOffsetLowerBound()
            throws Exception
    {
        try (TestingKafka testingKafka = TestingKafka.create()) {
            String topicName = createTopicWithMessages(testingKafka, "partition_offset");
            KafkaConfig config = baseConfig(testingKafka);
            KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);
            KafkaSplitManager splitManager = splitManager(config, internalFieldManager);

            assertThat(getSplits(
                    splitManager,
                    defaultSession(config),
                    tableHandle(topicName, constraint(
                            internalFieldManager,
                            Optional.of(domain(io.trino.spi.predicate.Range.equal(BIGINT, 1L))),
                            Optional.of(domain(io.trino.spi.predicate.Range.greaterThanOrEqual(BIGINT, 1L))),
                            Optional.empty()))))
                    .isNotEmpty();
        }
    }

    @Test
    public void testAcceptsExplicitPartitionAndTimestampLowerBound()
            throws Exception
    {
        try (TestingKafka testingKafka = TestingKafka.create()) {
            String topicName = createTopicWithMessages(testingKafka, "partition_timestamp");
            KafkaConfig config = baseConfig(testingKafka);
            KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);
            KafkaSplitManager splitManager = splitManager(config, internalFieldManager);

            assertThat(getSplits(
                    splitManager,
                    defaultSession(config),
                    tableHandle(topicName, constraint(
                            internalFieldManager,
                            Optional.of(domain(io.trino.spi.predicate.Range.equal(BIGINT, 1L))),
                            Optional.empty(),
                            Optional.of(domain(io.trino.spi.predicate.Range.greaterThanOrEqual(TIMESTAMP_MILLIS, 1_000_000L)))))))
                    .isNotEmpty();
        }
    }

    @Test
    public void testAcceptsPartitionInListAndOffsetLowerBound()
            throws Exception
    {
        try (TestingKafka testingKafka = TestingKafka.create()) {
            String topicName = createTopicWithMessages(testingKafka, "partition_in");
            KafkaConfig config = baseConfig(testingKafka);
            KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);
            KafkaSplitManager splitManager = splitManager(config, internalFieldManager);

            assertThat(getSplits(
                    splitManager,
                    defaultSession(config),
                    tableHandle(topicName, constraint(
                            internalFieldManager,
                            Optional.of(domain(io.trino.spi.predicate.Range.equal(BIGINT, 0L), io.trino.spi.predicate.Range.equal(BIGINT, 1L))),
                            Optional.of(domain(io.trino.spi.predicate.Range.greaterThanOrEqual(BIGINT, 1L))),
                            Optional.empty()))))
                    .hasSize(2);
        }
    }

    @Test
    public void testAllowUnscopedScanWhenEnforcementDisabled()
            throws Exception
    {
        try (TestingKafka testingKafka = TestingKafka.create()) {
            String topicName = createTopicWithMessages(testingKafka, "override");
            KafkaConfig config = baseConfig(testingKafka);
            KafkaSplitManager splitManager = splitManager(config);

            assertThat(getSplits(splitManager, defaultSession(config, false), tableHandle(topicName, TupleDomain.all())))
                    .isNotEmpty();
        }
    }

    @Test
    public void testCommittedReadModeUnaffected()
            throws Exception
    {
        try (TestingKafka testingKafka = TestingKafka.create()) {
            String topicName = createTopicWithMessages(testingKafka, "committed_read");
            KafkaConfig config = baseConfig(testingKafka)
                    .setCommittedReadMissingOffsetPolicy(KafkaCommittedReadMissingOffsetPolicy.EARLIEST);
            KafkaSplitManager splitManager = splitManager(config);

            assertThat(getSplits(splitManager, committedReadSession(config, "group-" + UUID.randomUUID()), tableHandle(topicName, TupleDomain.all())))
                    .isNotEmpty();
        }
    }

    @Test
    public void testConstraintNoneReturnsNoSplits()
            throws Exception
    {
        KafkaConfig config = baseConfig();
        KafkaInternalFieldManager internalFieldManager = new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, config);
        BrokerAccessTracker tracker = new BrokerAccessTracker();
        KafkaSplitManager splitManager = noBrokerSplitManager(config, internalFieldManager, tracker);

        assertThat(getSplits(splitManager, defaultSession(config), tableHandle(VALIDATION_ONLY_TOPIC, TupleDomain.none()))).isEmpty();
        assertThat(tracker.consumerFactoryUsed).isFalse();
        assertThat(tracker.adminFactoryUsed).isFalse();
    }

    private static String createTopicWithMessages(TestingKafka testingKafka, String prefix)
            throws Exception
    {
        testingKafka.start();
        String topicName = prefix + "_" + UUID.randomUUID().toString().replace("-", "_");
        testingKafka.createTopicWithConfig(2, 1, topicName, false);
        testingKafka.sendMessages(LongStream.range(0, 8).mapToObj(id -> new ProducerRecord<>(topicName, id, id)));
        return topicName;
    }

    private static KafkaConfig baseConfig()
    {
        return new KafkaConfig()
                .setNodes(java.util.Set.of("localhost:9092"))
                .setTableDescriptionSupplier("test");
    }

    private static KafkaConfig baseConfig(TestingKafka testingKafka)
    {
        return baseConfig()
                .setNodes(java.util.Set.of(testingKafka.getConnectString()));
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
        return splitManager(config, internalFieldManager, consumerFactory, adminFactory);
    }

    private static KafkaSplitManager noBrokerSplitManager(KafkaConfig config, KafkaInternalFieldManager internalFieldManager, BrokerAccessTracker tracker)
            throws Exception
    {
        KafkaConsumerFactory consumerFactory = new KafkaConsumerFactory()
        {
            @Override
            public Properties baseProperties(ConnectorSession session)
            {
                tracker.consumerFactoryUsed = true;
                throw new AssertionError("Kafka consumer should not be configured for scoped-read rejection");
            }

            @Override
            public String resolveReadPathGroupId(ConnectorSession session)
            {
                return "test-group";
            }
        };
        KafkaAdminFactory adminFactory = session -> {
            tracker.adminFactoryUsed = true;
            throw new AssertionError("Kafka admin should not be configured for scoped-read rejection");
        };
        return splitManager(config, internalFieldManager, consumerFactory, adminFactory);
    }

    private static KafkaSplitManager splitManager(
            KafkaConfig config,
            KafkaInternalFieldManager internalFieldManager,
            KafkaConsumerFactory consumerFactory,
            KafkaAdminFactory adminFactory)
            throws Exception
    {
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
        KafkaOffsetBoundsService offsetBoundsService = new KafkaOffsetBoundsService(consumerFactory, new MapBasedTableDescriptionSupplier(Map.of()));
        return new KafkaSplitManager(consumerFactory, adminFactory, config, filterManager, schemaProvider, new KafkaCommittedReadRegistry(), offsetBoundsService);
    }

    private static ConnectorSession defaultSession(KafkaConfig config)
    {
        return defaultSession(config, null);
    }

    private static ConnectorSession defaultSession(KafkaConfig config, Boolean enforceReadScope)
    {
        TestingConnectorSession.Builder sessionBuilder = TestingConnectorSession.builder()
                .setPropertyMetadata(new KafkaSessionProperties(config).getSessionProperties());
        if (enforceReadScope != null) {
            sessionBuilder.setPropertyValues(Map.of("enforce_read_scope", enforceReadScope));
        }
        return sessionBuilder.build();
    }

    private static ConnectorSession committedReadSession(KafkaConfig config, String groupId)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new KafkaSessionProperties(config).getSessionProperties())
                .setPropertyValues(Map.of(
                        "committed_read_enabled", true,
                        "consumer_group_id", groupId))
                .build();
    }

    private static KafkaTableHandle tableHandle(String topicName, TupleDomain<ColumnHandle> constraint)
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

    private static TupleDomain<ColumnHandle> partitionConstraint(KafkaInternalFieldManager internalFieldManager, io.trino.spi.predicate.Range... ranges)
    {
        return constraint(internalFieldManager, Optional.of(domain(ranges)), Optional.empty(), Optional.empty());
    }

    private static TupleDomain<ColumnHandle> partitionOffsetConstraint(KafkaInternalFieldManager internalFieldManager, io.trino.spi.predicate.Range... ranges)
    {
        return constraint(internalFieldManager, Optional.empty(), Optional.of(domain(ranges)), Optional.empty());
    }

    private static TupleDomain<ColumnHandle> timestampConstraint(KafkaInternalFieldManager internalFieldManager, io.trino.spi.predicate.Range... ranges)
    {
        return constraint(internalFieldManager, Optional.empty(), Optional.empty(), Optional.of(domain(ranges)));
    }

    private static TupleDomain<ColumnHandle> constraint(
            KafkaInternalFieldManager internalFieldManager,
            Optional<Domain> partitionDomain,
            Optional<Domain> partitionOffsetDomain,
            Optional<Domain> timestampDomain)
    {
        Map<ColumnHandle, Domain> domains = new HashMap<>();
        partitionDomain.ifPresent(domain -> domains.put(internalFieldManager.getFieldById(InternalFieldId.PARTITION_ID_FIELD).getColumnHandle(false), domain));
        partitionOffsetDomain.ifPresent(domain -> domains.put(internalFieldManager.getFieldById(InternalFieldId.PARTITION_OFFSET_FIELD).getColumnHandle(false), domain));
        timestampDomain.ifPresent(domain -> domains.put(internalFieldManager.getFieldById(InternalFieldId.OFFSET_TIMESTAMP_FIELD).getColumnHandle(false), domain));
        return TupleDomain.withColumnDomains(domains);
    }

    private static Domain domain(io.trino.spi.predicate.Range... ranges)
    {
        return Domain.create(ValueSet.ofRanges(Arrays.asList(ranges)), false);
    }

    private static void assertScopeRejectedWithoutBroker(KafkaConfig config, KafkaInternalFieldManager internalFieldManager, TupleDomain<ColumnHandle> constraint)
            throws Exception
    {
        BrokerAccessTracker tracker = new BrokerAccessTracker();
        KafkaSplitManager splitManager = noBrokerSplitManager(config, internalFieldManager, tracker);

        assertThatThrownBy(() -> getSplits(splitManager, defaultSession(config), tableHandle(VALIDATION_ONLY_TOPIC, constraint)))
                .hasMessageContaining("explicit finite predicate");
        assertThat(tracker.consumerFactoryUsed).isFalse();
        assertThat(tracker.adminFactoryUsed).isFalse();
    }

    private static class BrokerAccessTracker
    {
        private boolean consumerFactoryUsed;
        private boolean adminFactoryUsed;
    }
}
