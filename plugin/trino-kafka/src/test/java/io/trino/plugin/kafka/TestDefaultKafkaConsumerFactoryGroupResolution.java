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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDefaultKafkaConsumerFactoryGroupResolution
{
    @Test
    public void testDefaultModeUsesCatalogGroupIdWithoutSessionOverride()
            throws Exception
    {
        KafkaConfig config = config();

        Properties properties = new DefaultKafkaConsumerFactory(config).configure(session(config, false, null));

        assertThat(properties.getProperty(GROUP_ID_CONFIG)).isEqualTo("catalog-group");
    }

    @Test
    public void testDefaultModeUsesSessionGroupIdOverride()
            throws Exception
    {
        KafkaConfig config = config();

        Properties properties = new DefaultKafkaConsumerFactory(config).configure(session(config, false, "override-group"));

        assertThat(properties.getProperty(GROUP_ID_CONFIG)).isEqualTo("override-group");
    }

    @Test
    public void testCommittedReadModeRequiresSessionGroupId()
            throws Exception
    {
        KafkaConfig config = config();
        DefaultKafkaConsumerFactory factory = new DefaultKafkaConsumerFactory(config);

        assertThatThrownBy(() -> factory.configure(session(config, true, null)))
                .isInstanceOf(TrinoException.class)
                .extracting(throwable -> ((TrinoException) throwable).getErrorCode())
                .isEqualTo(INVALID_SESSION_PROPERTY.toErrorCode());
    }

    @Test
    public void testCommittedReadModeUsesSessionGroupId()
            throws Exception
    {
        KafkaConfig config = config();

        Properties properties = new DefaultKafkaConsumerFactory(config).configure(session(config, true, "cg"));

        assertThat(properties.getProperty(GROUP_ID_CONFIG)).isEqualTo("cg");
    }

    @Test
    public void testExplicitGroupConfigurationBypassesCommittedReadSessionRequirement()
            throws Exception
    {
        KafkaConfig config = config();

        Properties properties = new DefaultKafkaConsumerFactory(config).configureForGroup(session(config, true, null), "explicit-group");

        assertThat(properties.getProperty(GROUP_ID_CONFIG)).isEqualTo("explicit-group");
    }

    @Test
    public void testMetadataConfigurationBypassesCommittedReadSessionRequirement()
            throws Exception
    {
        KafkaConfig config = config();

        Properties properties = new DefaultKafkaConsumerFactory(config).configureForMetadata(session(config, true, null));

        assertThat(properties.getProperty(GROUP_ID_CONFIG)).isEqualTo(KafkaConsumerFactory.METADATA_GROUP_ID_PLACEHOLDER);
    }

    @Test
    public void testBasePropertiesDoNotIncludeGroupId()
            throws Exception
    {
        KafkaConfig config = config();

        Properties defaultModeProperties = new DefaultKafkaConsumerFactory(config).baseProperties(session(config, false, "override-group"));
        Properties committedReadProperties = new DefaultKafkaConsumerFactory(config).baseProperties(session(config, true, null));

        assertThat(defaultModeProperties).doesNotContainKey(GROUP_ID_CONFIG);
        assertThat(committedReadProperties).doesNotContainKey(GROUP_ID_CONFIG);
    }

    private static KafkaConfig config()
    {
        return new KafkaConfig()
                .setNodes(java.util.Set.of("localhost:9092"))
                .setConsumerGroupId("catalog-group");
    }

    private static ConnectorSession session(KafkaConfig config, boolean committedReadEnabled, String consumerGroupId)
    {
        Map<String, Object> propertyValues = new HashMap<>();
        propertyValues.put("committed_read_enabled", committedReadEnabled);
        if (consumerGroupId != null) {
            propertyValues.put("consumer_group_id", consumerGroupId);
        }
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new KafkaSessionProperties(config).getSessionProperties())
                .setPropertyValues(propertyValues)
                .build();
    }
}
