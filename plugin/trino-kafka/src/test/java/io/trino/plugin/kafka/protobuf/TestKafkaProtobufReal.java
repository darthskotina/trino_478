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
package io.trino.plugin.kafka.protobuf;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class TestKafkaProtobufReal
{
    @Test
    public void testExistingCatalog()
            throws Exception
    {
        Class.forName("io.trino.jdbc.TrinoDriver");
        String url = "jdbc:trino://localhost:8080";

        try (Connection connection = DriverManager.getConnection(url, "test_user", null);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select source_type, transform(ticks, t -> t.bid.value) as bid_values, _partition_offset from kafka_real.default.b2c_mt4_ticks_events_reals_real limit 3")) {
            assertThat(resultSet.next())
                    .as("Expected at least one row from the existing catalog")
                    .isTrue();
            System.out.println("Source Type: " + resultSet.getString("source_type"));
            System.out.println("Bid Values: " + resultSet.getObject("bid_values"));
        }
    }
}
