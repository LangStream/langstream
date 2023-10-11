/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.functions.transforms;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public final class JsonNodeSchema implements Schema<JsonNode> {
    private static final ThreadLocal<ObjectMapper> JSON_MAPPER =
            ThreadLocal.withInitial(
                    () -> {
                        ObjectMapper mapper = new ObjectMapper();
                        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                        return mapper;
                    });
    private final org.apache.avro.Schema nativeSchema;
    private final SchemaInfo schemaInfo;

    public JsonNodeSchema(org.apache.avro.Schema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("Avro schema cannot be null");
        }
        this.nativeSchema = schema;
        this.schemaInfo =
                SchemaInfo.builder()
                        .name("")
                        .type(SchemaType.JSON)
                        .schema(schema.toString().getBytes(StandardCharsets.UTF_8))
                        .build();
    }

    @Override
    public byte[] encode(JsonNode message) {
        try {
            return JSON_MAPPER.get().writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public JsonNode decode(byte[] bytes, byte[] schemaVersion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(this.nativeSchema);
    }

    @Override
    public Schema clone() {
        return new JsonNodeSchema(nativeSchema);
    }
}
