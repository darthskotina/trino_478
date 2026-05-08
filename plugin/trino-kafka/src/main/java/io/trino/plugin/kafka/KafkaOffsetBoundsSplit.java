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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;

public class KafkaOffsetBoundsSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(KafkaOffsetBoundsSplit.class);

    private final int partitionId;
    private final long logStartOffset;
    private final long logEndOffset;

    public KafkaOffsetBoundsSplit(KafkaOffsetBounds offsetBounds)
    {
        this(offsetBounds.partitionId(), offsetBounds.logStartOffset(), offsetBounds.logEndOffset());
    }

    @JsonCreator
    public KafkaOffsetBoundsSplit(
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("logStartOffset") long logStartOffset,
            @JsonProperty("logEndOffset") long logEndOffset)
    {
        this.partitionId = partitionId;
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
    }

    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public long getLogStartOffset()
    {
        return logStartOffset;
    }

    @JsonProperty
    public long getLogEndOffset()
    {
        return logEndOffset;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
