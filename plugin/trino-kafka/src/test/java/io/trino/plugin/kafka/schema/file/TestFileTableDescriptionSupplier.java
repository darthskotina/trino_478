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
package io.trino.plugin.kafka.schema.file;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.trino.plugin.kafka.KafkaConfig;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.plugin.kafka.util.CodecSupplier;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFileTableDescriptionSupplier
{
    @Test
    public void testLoadDataOffset()
    {
        File metadataDir = new File(getResource("read_test").getPath());
        FileTableDescriptionSupplierConfig config = new FileTableDescriptionSupplierConfig()
                .setTableDescriptionDir(metadataDir)
                .setTableNames(ImmutableSet.of("read_test.all_datatypes_protobuf_data_offset"));

        KafkaConfig kafkaConfig = new KafkaConfig();
        JsonCodec<KafkaTopicDescription> topicDescriptionCodec = new CodecSupplier<>(KafkaTopicDescription.class, TESTING_TYPE_MANAGER).get();

        FileTableDescriptionSupplier supplier = new FileTableDescriptionSupplier(config, kafkaConfig, topicDescriptionCodec);
        TableDescriptionSupplier tableDescriptionSupplier = supplier.get();

        Optional<KafkaTopicDescription> description = tableDescriptionSupplier.getTopicDescription(TestingConnectorSession.SESSION, new SchemaTableName("read_test", "all_datatypes_protobuf_data_offset"));
        assertThat(description).isPresent();
        assertThat(description.get().message()).isPresent();
        assertThat(description.get().message().get().dataOffset()).contains(5);
    }
}
