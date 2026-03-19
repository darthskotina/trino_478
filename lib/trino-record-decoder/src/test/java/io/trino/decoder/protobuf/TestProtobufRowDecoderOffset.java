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
package io.trino.decoder.protobuf;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.DecoderTestColumnHandle;
import io.trino.decoder.FieldValueProvider;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static io.trino.decoder.util.DecoderTestUtil.checkValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestProtobufRowDecoderOffset
{
    @Test
    public void testDataOffset()
            throws Exception
    {
        String stringData = "TrinoWithOffset";
        Descriptor descriptor = ProtobufUtils.getFileDescriptor(ProtobufUtils.getProtoFile("decoder/protobuf/all_datatypes.proto")).findMessageTypeByName(DEFAULT_MESSAGE);
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);
        messageBuilder.setField(descriptor.findFieldByName("stringColumn"), stringData);
        byte[] messageData = messageBuilder.build().toByteArray();

        // Add some garbage prefix
        int offset = 5;
        byte[] dataWithOffset = new byte[messageData.length + offset];
        System.arraycopy(messageData, 0, dataWithOffset, offset, messageData.length);
        for (int i = 0; i < offset; i++) {
            dataWithOffset[i] = (byte) 0xFF;
        }

        DecoderTestColumnHandle stringColumn = new DecoderTestColumnHandle(0, "stringColumn", VARCHAR, "stringColumn", null, null, false, false, false);

        ProtobufRowDecoder decoder = new ProtobufRowDecoder(
                new FixedSchemaDynamicMessageProvider(descriptor),
                ImmutableSet.of(stringColumn),
                TESTING_TYPE_MANAGER,
                new FileDescriptorProvider(),
                Optional.of(offset));

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = decoder
                .decodeRow(dataWithOffset)
                .orElseThrow(AssertionError::new);

        assertThat(decodedRow).hasSize(1);
        checkValue(decodedRow, stringColumn, stringData);
    }
}
