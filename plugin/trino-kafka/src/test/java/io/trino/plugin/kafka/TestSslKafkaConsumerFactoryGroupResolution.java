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

import io.trino.plugin.kafka.security.KafkaSslConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_RACK_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSslKafkaConsumerFactoryGroupResolution
{
    @Test
    public void testSslOverlayAppliesToAllFactoryPaths()
            throws Exception
    {
        KafkaConfig kafkaConfig = new KafkaConfig()
                .setNodes(java.util.Set.of("localhost:9092"))
                .setConsumerGroupId("catalog-group");
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new KafkaSessionProperties(kafkaConfig).getSessionProperties())
                .setPropertyValues(Map.of(
                        "committed_read_enabled", false,
                        "client_rack", "rack-ssl"))
                .build();
        KafkaSslConfig sslConfig = new KafkaSslConfig()
                .setTruststoreLocation("/some/path/to/truststore")
                .setTruststorePassword("truststore-password");

        SslKafkaConsumerFactory factory = new SslKafkaConsumerFactory(new DefaultKafkaConsumerFactory(kafkaConfig), sslConfig);

        Properties base = factory.baseProperties(session);
        Properties read = factory.configure(session);
        Properties group = factory.configureForGroup(session, "explicit-group");
        Properties metadata = factory.configureForMetadata(session);

        assertSslProperties(base);
        assertSslProperties(read);
        assertSslProperties(group);
        assertSslProperties(metadata);
        assertThat(read.getProperty(GROUP_ID_CONFIG)).isEqualTo("catalog-group");
        assertThat(group.getProperty(GROUP_ID_CONFIG)).isEqualTo("explicit-group");
        assertThat(metadata.getProperty(GROUP_ID_CONFIG)).isEqualTo(KafkaConsumerFactory.METADATA_GROUP_ID_PLACEHOLDER);
        assertClientRack(base);
        assertClientRack(read);
        assertClientRack(group);
        assertClientRack(metadata);
    }

    private static void assertSslProperties(Properties properties)
    {
        assertThat(properties).containsEntry(SSL_TRUSTSTORE_LOCATION_CONFIG, "/some/path/to/truststore");
        assertThat(properties).containsEntry(SECURITY_PROTOCOL_CONFIG, SSL.name());
    }

    private static void assertClientRack(Properties properties)
    {
        assertThat(properties).containsEntry(CLIENT_RACK_CONFIG, "rack-ssl");
    }
}
