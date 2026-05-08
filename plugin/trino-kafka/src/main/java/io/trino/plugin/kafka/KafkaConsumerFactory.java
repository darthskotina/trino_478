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

import io.trino.spi.connector.ConnectorSession;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public interface KafkaConsumerFactory
{
    String METADATA_GROUP_ID_PLACEHOLDER = "trino-kafka-metadata";

    Properties baseProperties(ConnectorSession session);

    default Properties configure(ConnectorSession session)
    {
        Properties properties = baseProperties(session);
        properties.setProperty(GROUP_ID_CONFIG, resolveReadPathGroupId(session));
        return properties;
    }

    default Properties configureForGroup(ConnectorSession session, String groupId)
    {
        requireNonNull(groupId, "groupId is null");
        Properties properties = baseProperties(session);
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        return properties;
    }

    default Properties configureForMetadata(ConnectorSession session)
    {
        Properties properties = baseProperties(session);
        properties.setProperty(GROUP_ID_CONFIG, METADATA_GROUP_ID_PLACEHOLDER);
        return properties;
    }

    String resolveReadPathGroupId(ConnectorSession session);

    default KafkaConsumer<byte[], byte[]> create(ConnectorSession session)
    {
        return new KafkaConsumer<>(configure(session));
    }

    default KafkaConsumer<byte[], byte[]> createForGroup(ConnectorSession session, String groupId)
    {
        return new KafkaConsumer<>(configureForGroup(session, groupId));
    }

    default KafkaConsumer<byte[], byte[]> createForMetadata(ConnectorSession session)
    {
        return new KafkaConsumer<>(configureForMetadata(session));
    }
}
