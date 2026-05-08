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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.lang.String.format;

public final class KafkaSessionProperties
        implements SessionPropertiesProvider
{
    private static final String TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED = "timestamp_upper_bound_force_push_down_enabled";
    private static final String ENFORCE_READ_SCOPE = "enforce_read_scope";
    private static final String COMMITTED_READ_ENABLED = "committed_read_enabled";
    private static final String CONSUMER_GROUP_ID = "consumer_group_id";
    private static final String COMMITTED_READ_ALLOW_OFFSET_REWIND = "committed_read_allow_offset_rewind";
    private static final String COMMITTED_READ_MAX_ROWS_PER_PARTITION = "committed_read_max_rows_per_partition";
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public KafkaSessionProperties(KafkaConfig kafkaConfig)
    {
        sessionProperties = ImmutableList.of(
                PropertyMetadata.booleanProperty(
                        TIMESTAMP_UPPER_BOUND_FORCE_PUSH_DOWN_ENABLED,
                        "Enable or disable timestamp upper bound push down for topic createTime mode",
                        kafkaConfig.isTimestampUpperBoundPushDownEnabled(), false),
                PropertyMetadata.booleanProperty(
                        ENFORCE_READ_SCOPE,
                        "Require scoped predicates for normal-mode Kafka reads",
                        kafkaConfig.isEnforceReadScope(),
                        false),
                PropertyMetadata.booleanProperty(
                        COMMITTED_READ_ENABLED,
                        "Enable or disable committed-read mode",
                        kafkaConfig.isCommittedReadEnabled(), false),
                stringProperty(
                        CONSUMER_GROUP_ID,
                        "Kafka consumer group ID; required by committed-read mode and overrides the legacy default-mode group ID when set",
                        null,
                        value -> {
                            if (value != null && value.isBlank()) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property '%s' must not be blank", CONSUMER_GROUP_ID));
                            }
                        },
                        false),
                PropertyMetadata.booleanProperty(
                        COMMITTED_READ_ALLOW_OFFSET_REWIND,
                        "Allow committed-read mode to honor explicit _partition_offset lower bounds and commit a lower offset window when fully consumed",
                        false,
                        false),
                longProperty(
                        COMMITTED_READ_MAX_ROWS_PER_PARTITION,
                        "Limit committed-read split planning to at most this many source offsets per selected partition; 0 means unlimited",
                        0L,
                        value -> {
                            if (value < 0) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property '%s' must be greater than or equal to 0", COMMITTED_READ_MAX_ROWS_PER_PARTITION));
                            }
                        },
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
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

    public static boolean isCommittedReadEnabled(ConnectorSession session)
    {
        return session.getProperty(COMMITTED_READ_ENABLED, Boolean.class);
    }

    public static boolean isEnforceReadScope(ConnectorSession session)
    {
        return session.getProperty(ENFORCE_READ_SCOPE, Boolean.class);
    }

    public static Optional<String> getConsumerGroupIdSessionProperty(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(CONSUMER_GROUP_ID, String.class));
    }

    public static boolean isCommittedReadAllowOffsetRewind(ConnectorSession session)
    {
        return session.getProperty(COMMITTED_READ_ALLOW_OFFSET_REWIND, Boolean.class);
    }

    public static long getCommittedReadMaxRowsPerPartition(ConnectorSession session)
    {
        return session.getProperty(COMMITTED_READ_MAX_ROWS_PER_PARTITION, Long.class);
    }

    public static String getRequiredCommittedReadGroupId(ConnectorSession session)
    {
        return getConsumerGroupIdSessionProperty(session)
                .orElseThrow(() -> new TrinoException(
                        INVALID_SESSION_PROPERTY,
                        format("Committed-read mode requires session property '%s' to be set", CONSUMER_GROUP_ID)));
    }
}
