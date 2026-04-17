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
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.longProperty;
import static java.lang.String.format;

public final class KafkaSessionProperties
        implements SessionPropertiesProvider
{
    private static final String REQUIRE_FILTER = "require_filter";
    private static final String MAX_READ_OFFSETS = "max_read_offsets";
    private static final String TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED = "timestamp_upper_bound_force_push_down_enabled";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public KafkaSessionProperties(KafkaConfig kafkaConfig)
    {
        sessionProperties = ImmutableList.of(
                PropertyMetadata.booleanProperty(
                        REQUIRE_FILTER,
                        "Require a WHERE clause before reading from Kafka topics",
                        kafkaConfig.isRequireFilter(),
                        false),
                longProperty(
                        MAX_READ_OFFSETS,
                        "Maximum number of Kafka offsets a query may scan before failing. Set to 0 to disable",
                        kafkaConfig.getMaxReadOffsets(),
                        value -> validateNonNegativeLongValue(value, MAX_READ_OFFSETS),
                        false),
                PropertyMetadata.booleanProperty(
                        TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED,
                        "Enable or disable timestamp upper bound push down for topic createTime mode",
                        kafkaConfig.isTimestampUpperBoundPushDownEnabled(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isRequireFilter(ConnectorSession session)
    {
        return session.getProperty(REQUIRE_FILTER, Boolean.class);
    }

    public static long getMaxReadOffsets(ConnectorSession session)
    {
        return session.getProperty(MAX_READ_OFFSETS, Long.class);
    }

    /**
     * If predicate specifies lower bound on _timestamp column (_timestamp > XXXX), it is always pushed down.
     * The upper bound predicate is pushed down only for topics using ``LogAppendTime`` mode.
     * For topics using ``CreateTime`` mode, upper bound push down must be explicitly
     * allowed via ``kafka.timestamp-upper-bound-force-push-down-enabled`` config property
     * or ``timestamp_upper_bound_force_push_down_enabled`` session property.
     */
    public static boolean isTimestampUpperBoundPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED, Boolean.class);
    }

    private static void validateNonNegativeLongValue(long value, String propertyName)
    {
        if (value < 0) {
            throw new IllegalArgumentException(format("%s must be greater than or equal to 0", propertyName));
        }
    }
}
