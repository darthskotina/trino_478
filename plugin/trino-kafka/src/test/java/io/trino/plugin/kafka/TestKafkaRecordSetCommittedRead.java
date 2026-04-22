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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.testing.TestingConnectorSession;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKafkaRecordSetCommittedRead
{
    @Test
    public void testCommitFailurePropagates()
    {
        FailingCommitConsumer consumer = new FailingCommitConsumer();
        RecordCursor cursor = recordSet(consumer, checkpointOnlySplit()).cursor();

        assertThatThrownBy(cursor::close)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to commit Kafka offset 7");
        assertThat(consumer.commitCalls).isEqualTo(1);
        assertThat(consumer.closed).isTrue();
    }

    @Test
    public void testCommitFailurePropagatesForFullyConsumedCappedDataSplit()
    {
        FailingCommitConsumer consumer = new FailingCommitConsumer();
        RecordCursor cursor = recordSet(consumer, cappedDataSplit()).cursor();

        assertThat(cursor.advanceNextPosition()).isFalse();
        assertThatThrownBy(cursor::close)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to commit Kafka offset 7");
        assertThat(consumer.commitCalls).isEqualTo(1);
        assertThat(consumer.closed).isTrue();
    }

    @Test
    public void testRuntimeOffsetInvalidationSuppressesCommit()
    {
        OffsetOutOfRangeTestConsumer consumer = new OffsetOutOfRangeTestConsumer();
        RecordCursor cursor = recordSet(consumer, dataSplit()).cursor();

        assertThatThrownBy(cursor::advanceNextPosition)
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("became invalid during execution");

        cursor.close();
        assertThat(consumer.commitCalls).isZero();
        assertThat(consumer.closed).isTrue();
    }

    @Test
    public void testInterruptedCloseSuppressesCommit()
    {
        TrackingCommitConsumer consumer = new TrackingCommitConsumer();
        RecordCursor cursor = recordSet(consumer, checkpointOnlySplit()).cursor();

        Thread.currentThread().interrupt();
        try {
            cursor.close();
        }
        finally {
            Thread.interrupted();
        }

        assertThat(consumer.commitCalls).isZero();
        assertThat(consumer.closed).isTrue();
    }

    @Test
    public void testRegularSplitCloseBeforeEndSuppressesCommit()
    {
        TrackingCommitConsumer consumer = new TrackingCommitConsumer(7);
        RecordCursor cursor = recordSet(consumer, dataSplit()).cursor();

        cursor.close();

        assertThat(consumer.commitCalls).isZero();
        assertThat(consumer.closed).isTrue();
    }

    @Test
    public void testFullyConsumedCappedSplitCommitsPlannedBatchEnd()
    {
        ConsumingCommitConsumer consumer = new ConsumingCommitConsumer(5, 6);
        RecordCursor cursor = recordSet(consumer, cappedDataSplit()).cursor();

        assertThat(cursor.advanceNextPosition()).isTrue();
        assertThat(cursor.advanceNextPosition()).isTrue();
        assertThat(cursor.advanceNextPosition()).isFalse();
        cursor.close();

        assertThat(consumer.commitCalls).isEqualTo(1);
        assertThat(consumer.lastCommittedOffset).isEqualTo(7L);
        assertThat(consumer.closed).isTrue();
    }

    @Test
    public void testCappedSplitEarlyCloseSuppressesCommit()
    {
        TrackingCommitConsumer consumer = new TrackingCommitConsumer(7);
        RecordCursor cursor = recordSet(consumer, cappedDataSplit()).cursor();

        cursor.close();

        assertThat(consumer.commitCalls).isZero();
        assertThat(consumer.closed).isTrue();
    }

    @Test
    public void testFullyConsumedRewindSplitCommitsWindowEnd()
    {
        ConsumingCommitConsumer consumer = new ConsumingCommitConsumer(5);
        RecordCursor cursor = recordSet(consumer, rewindDataSplit()).cursor();

        assertThat(cursor.advanceNextPosition()).isTrue();
        assertThat(cursor.advanceNextPosition()).isFalse();
        cursor.close();

        assertThat(consumer.commitCalls).isEqualTo(1);
        assertThat(consumer.lastCommittedOffset).isEqualTo(6L);
        assertThat(consumer.closed).isTrue();
    }

    @Test
    public void testEqualityRewindSplitWithLargerCapStillCommitsSingleRowEnd()
    {
        ConsumingCommitConsumer consumer = new ConsumingCommitConsumer(5);
        RecordCursor cursor = recordSet(consumer, equalityRewindSplitWithLargeCap()).cursor();

        assertThat(cursor.advanceNextPosition()).isTrue();
        assertThat(cursor.advanceNextPosition()).isFalse();
        cursor.close();

        assertThat(consumer.commitCalls).isEqualTo(1);
        assertThat(consumer.lastCommittedOffset).isEqualTo(6L);
        assertThat(consumer.closed).isTrue();
    }

    @Test
    public void testRewindSplitEarlyCloseSuppressesCommit()
    {
        TrackingCommitConsumer consumer = new TrackingCommitConsumer(6);
        RecordCursor cursor = recordSet(consumer, rewindDataSplit()).cursor();

        cursor.close();

        assertThat(consumer.commitCalls).isZero();
        assertThat(consumer.closed).isTrue();
    }

    private static KafkaRecordSet recordSet(KafkaConsumer<byte[], byte[]> consumer, KafkaSplit split)
    {
        KafkaConsumerFactory stubFactory = new KafkaConsumerFactory()
        {
            @Override
            public KafkaConsumer<byte[], byte[]> create(io.trino.spi.connector.ConnectorSession session)
            {
                return consumer;
            }

            @Override
            public Properties configure(io.trino.spi.connector.ConnectorSession session)
            {
                return new Properties();
            }
        };
        return new KafkaRecordSet(
                split,
                stubFactory,
                TestingConnectorSession.SESSION,
                List.of(),
                new DummyRowDecoder(),
                new DummyRowDecoder(),
                new KafkaInternalFieldManager(TESTING_TYPE_MANAGER, new KafkaConfig()));
    }

    private static KafkaSplit checkpointOnlySplit()
    {
        return new KafkaSplit(
                "checkpoint_topic",
                DummyRowDecoder.NAME,
                DummyRowDecoder.NAME,
                Optional.empty(),
                Optional.empty(),
                0,
                new Range(7, 7),
                Optional.of(new KafkaCommittedReadSplitMetadata("group-a", true, 7)),
                io.trino.spi.HostAddress.fromString("localhost:9092"));
    }

    private static KafkaSplit dataSplit()
    {
        return new KafkaSplit(
                "data_topic",
                DummyRowDecoder.NAME,
                DummyRowDecoder.NAME,
                Optional.empty(),
                Optional.empty(),
                0,
                new Range(5, 10),
                Optional.of(new KafkaCommittedReadSplitMetadata("group-b", false, 10)),
                io.trino.spi.HostAddress.fromString("localhost:9092"));
    }

    private static KafkaSplit rewindDataSplit()
    {
        return equalityRewindSplitWithLargeCap();
    }

    private static KafkaSplit cappedDataSplit()
    {
        return new KafkaSplit(
                "data_topic",
                DummyRowDecoder.NAME,
                DummyRowDecoder.NAME,
                Optional.empty(),
                Optional.empty(),
                0,
                new Range(5, 7),
                Optional.of(new KafkaCommittedReadSplitMetadata("group-capped", false, 7)),
                io.trino.spi.HostAddress.fromString("localhost:9092"));
    }

    private static KafkaSplit equalityRewindSplitWithLargeCap()
    {
        return new KafkaSplit(
                "data_topic",
                DummyRowDecoder.NAME,
                DummyRowDecoder.NAME,
                Optional.empty(),
                Optional.empty(),
                0,
                new Range(5, 6),
                Optional.of(new KafkaCommittedReadSplitMetadata("group-rewind", false, 6)),
                io.trino.spi.HostAddress.fromString("localhost:9092"));
    }

    private static class FailingCommitConsumer
            extends KafkaConsumer<byte[], byte[]>
    {
        private int commitCalls;
        private boolean closed;

        private FailingCommitConsumer()
        {
            super(minimalConsumerProperties());
        }

        @Override
        public void assign(java.util.Collection<TopicPartition> partitions) {}

        @Override
        public void seek(TopicPartition partition, long offset) {}

        @Override
        public ConsumerRecords<byte[], byte[]> poll(Duration timeout)
        {
            return ConsumerRecords.empty();
        }

        @Override
        public long position(TopicPartition partition)
        {
            return 7;
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)
        {
            commitCalls++;
            throw new KafkaException("commit failed");
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static class OffsetOutOfRangeTestConsumer
            extends KafkaConsumer<byte[], byte[]>
    {
        private int commitCalls;
        private boolean closed;

        private OffsetOutOfRangeTestConsumer()
        {
            super(minimalConsumerProperties());
        }

        @Override
        public void assign(java.util.Collection<TopicPartition> partitions) {}

        @Override
        public void seek(TopicPartition partition, long offset) {}

        @Override
        public ConsumerRecords<byte[], byte[]> poll(Duration timeout)
        {
            throw new OffsetOutOfRangeException(Map.of(new TopicPartition("data_topic", 0), 5L));
        }

        @Override
        public long position(TopicPartition partition)
        {
            return 5;
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)
        {
            commitCalls++;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static class TrackingCommitConsumer
            extends KafkaConsumer<byte[], byte[]>
    {
        private final long position;
        private int commitCalls;
        private boolean closed;

        private TrackingCommitConsumer()
        {
            this(7);
        }

        private TrackingCommitConsumer(long position)
        {
            super(minimalConsumerProperties());
            this.position = position;
        }

        @Override
        public void assign(java.util.Collection<TopicPartition> partitions) {}

        @Override
        public void seek(TopicPartition partition, long offset) {}

        @Override
        public ConsumerRecords<byte[], byte[]> poll(Duration timeout)
        {
            return ConsumerRecords.empty();
        }

        @Override
        public long position(TopicPartition partition)
        {
            return position;
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)
        {
            commitCalls++;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static class ConsumingCommitConsumer
            extends KafkaConsumer<byte[], byte[]>
    {
        private final TopicPartition topicPartition = new TopicPartition("data_topic", 0);
        private final ConsumerRecords<byte[], byte[]> records;
        private final long finalPosition;
        private boolean polled;
        private int commitCalls;
        private long lastCommittedOffset = -1;
        private boolean closed;

        private ConsumingCommitConsumer(long... offsets)
        {
            super(minimalConsumerProperties());
            Map<TopicPartition, List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>>> recordsMap = Map.of(
                    topicPartition,
                    java.util.Arrays.stream(offsets)
                            .mapToObj(offset -> new org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>(
                                    "data_topic",
                                    0,
                                    offset,
                                    null,
                                    null))
                            .toList());
            this.records = new ConsumerRecords<>(recordsMap, Map.of(topicPartition, new OffsetAndMetadata(offsets[offsets.length - 1] + 1)));
            this.finalPosition = offsets[offsets.length - 1] + 1;
        }

        @Override
        public void assign(java.util.Collection<TopicPartition> partitions) {}

        @Override
        public void seek(TopicPartition partition, long offset) {}

        @Override
        public ConsumerRecords<byte[], byte[]> poll(Duration timeout)
        {
            if (polled) {
                return ConsumerRecords.empty();
            }
            polled = true;
            return records;
        }

        @Override
        public long position(TopicPartition partition)
        {
            return polled ? finalPosition : 5;
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)
        {
            commitCalls++;
            lastCommittedOffset = offsets.get(topicPartition).offset();
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static Properties minimalConsumerProperties()
    {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(GROUP_ID_CONFIG, "test-group");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }
}
