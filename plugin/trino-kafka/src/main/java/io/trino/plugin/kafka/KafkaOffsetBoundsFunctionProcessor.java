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
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.type.BigintType.BIGINT;

public class KafkaOffsetBoundsFunctionProcessor
        implements TableFunctionSplitProcessor
{
    private final KafkaOffsetBoundsSplit split;
    private boolean finished;

    public KafkaOffsetBoundsFunctionProcessor(KafkaOffsetBoundsSplit split)
    {
        this.split = split;
    }

    @Override
    public TableFunctionProcessorState process()
    {
        if (finished) {
            return FINISHED;
        }

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT));
        checkState(pageBuilder.isEmpty(), "pageBuilder is not empty");

        pageBuilder.declarePosition();
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), split.getPartitionId());
        BIGINT.writeLong(pageBuilder.getBlockBuilder(1), split.getLogStartOffset());
        BIGINT.writeLong(pageBuilder.getBlockBuilder(2), split.getLogEndOffset());
        if (split.getLogStartOffset() == split.getLogEndOffset()) {
            pageBuilder.getBlockBuilder(3).appendNull();
        }
        else {
            BIGINT.writeLong(pageBuilder.getBlockBuilder(3), split.getLogEndOffset() - 1);
        }

        finished = true;
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return produced(page);
    }
}
