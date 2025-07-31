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
package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class VerticaSessionProperties
        implements SessionPropertiesProvider
{
    public static final String CONVERT_DECIMAL_TO_VARCHAR = "enable_convert_decimal_to_varchar";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public VerticaSessionProperties(VerticaConfig verticaConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        CONVERT_DECIMAL_TO_VARCHAR,
                        "Enable converting decimal columns to varchar",
                        verticaConfig.isEnableConvertDecimalToVarchar(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isEnableConvertDecimalToVarchar(ConnectorSession session)
    {
        return session.getProperty(CONVERT_DECIMAL_TO_VARCHAR, Boolean.class);
    }
}
