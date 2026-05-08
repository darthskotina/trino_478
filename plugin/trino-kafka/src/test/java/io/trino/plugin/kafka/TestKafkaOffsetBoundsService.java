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
import org.junit.jupiter.api.Test;

import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestKafkaOffsetBoundsService
{
    @Test
    public void testMissingBrokerTopic()
    {
        assertThatThrownBy(() -> KafkaOffsetBoundsService.requirePartitionInfo("missing-topic", null))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Topic 'missing-topic' was not found on the broker")
                .extracting(throwable -> ((TrinoException) throwable).getErrorCode())
                .isEqualTo(KAFKA_SPLIT_ERROR.toErrorCode());
    }
}
