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
package io.trino.plugin.kafka.ptf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.kafka.KafkaOffsetBoundsFunctionHandle;
import io.trino.plugin.kafka.KafkaOffsetBoundsService;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.base.util.Functions.checkFunctionArgument;
import static io.trino.spi.function.table.Descriptor.descriptor;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Integer.MAX_VALUE;
import static java.util.Objects.requireNonNull;

public class OffsetBoundsFunction
        implements Provider<ConnectorTableFunction>
{
    private final KafkaOffsetBoundsService offsetBoundsService;

    @Inject
    public OffsetBoundsFunction(KafkaOffsetBoundsService offsetBoundsService)
    {
        this.offsetBoundsService = requireNonNull(offsetBoundsService, "offsetBoundsService is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new OffsetBoundsTableFunction(offsetBoundsService), getClass().getClassLoader());
    }

    public static class OffsetBoundsTableFunction
            extends AbstractConnectorTableFunction
    {
        private static final String FUNCTION_SCHEMA = "system";
        private static final String FUNCTION_NAME = "offsets";
        private static final String SCHEMA_NAME_ARGUMENT = "SCHEMA_NAME";
        private static final String TABLE_NAME_ARGUMENT = "TABLE_NAME";
        private static final String PARTITION_ARGUMENT = "PARTITION";

        private final KafkaOffsetBoundsService offsetBoundsService;

        public OffsetBoundsTableFunction(KafkaOffsetBoundsService offsetBoundsService)
        {
            super(
                    FUNCTION_SCHEMA,
                    FUNCTION_NAME,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder().name(SCHEMA_NAME_ARGUMENT).type(VARCHAR).build(),
                            ScalarArgumentSpecification.builder().name(TABLE_NAME_ARGUMENT).type(VARCHAR).build(),
                            ScalarArgumentSpecification.builder().name(PARTITION_ARGUMENT).type(BIGINT).defaultValue(null).build()),
                    new DescribedTable(descriptor(
                            ImmutableList.of("partition_id", "log_start_offset", "log_end_offset", "last_readable_offset"),
                            ImmutableList.of(BIGINT, BIGINT, BIGINT, BIGINT))));
            this.offsetBoundsService = requireNonNull(offsetBoundsService, "offsetBoundsService is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            String schemaName = getRequiredStringArgument(arguments, SCHEMA_NAME_ARGUMENT, "schema_name");
            String tableName = getRequiredStringArgument(arguments, TABLE_NAME_ARGUMENT, "table_name");
            Optional<Integer> partition = getPartition(arguments);

            SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
            KafkaTopicDescription topicDescription = offsetBoundsService.getTopicDescription(session, schemaTableName)
                    .orElseThrow(() -> new TableNotFoundException(schemaTableName));

            accessControl.checkCanSelectFromColumns(null, schemaTableName, ImmutableSet.of());

            return TableFunctionAnalysis.builder()
                    .handle(new KafkaOffsetBoundsFunctionHandle(schemaTableName, topicDescription.topicName(), partition))
                    .build();
        }

        private static String getRequiredStringArgument(Map<String, Argument> arguments, String argumentName, String displayName)
        {
            ScalarArgument argument = (ScalarArgument) arguments.get(argumentName);
            checkFunctionArgument(argument.getValue() != null, "%s cannot be null", displayName);
            String value = ((Slice) argument.getValue()).toStringUtf8();
            checkFunctionArgument(!value.isBlank(), "%s cannot be blank", displayName);
            return value;
        }

        private static Optional<Integer> getPartition(Map<String, Argument> arguments)
        {
            ScalarArgument argument = (ScalarArgument) arguments.get(PARTITION_ARGUMENT);
            if (argument.getValue() == null) {
                return Optional.empty();
            }

            long partition = (long) argument.getValue();
            checkFunctionArgument(partition >= 0, "partition must not be negative");
            checkFunctionArgument(partition <= MAX_VALUE, "partition must be less than or equal to %s", MAX_VALUE);
            return Optional.of((int) partition);
        }
    }
}
