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
package ai.langstream.api.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;

public class ResourcesSpecDeserializer extends StdDeserializer<ResourcesSpec> {

    public ResourcesSpecDeserializer() {
        super(ResourcesSpec.class);
    }

    @Override
    public ResourcesSpec deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        Object parallelism = parseField(node, "parallelism");
        Object size = parseField(node, "size");
        DiskSpec disk = null; // Allow disk to be null

        if (node.has("disk") && !node.get("disk").isNull()) {
            disk = parseDiskSpec(node.get("disk"), ctxt, p);
        }

        return new ResourcesSpec(parallelism, size, disk);
    }

    private Object parseField(JsonNode node, String fieldName) {
        if (node == null || !node.has(fieldName)) {
            return null;
        }
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode.isInt()) {
            return fieldNode.asInt();
        } else if (fieldNode.isTextual()) {
            String textValue = fieldNode.asText();
            if (textValue.matches("\\$\\{.*\\}")) {
                return textValue;
            }
            try {
                return Integer.parseInt(textValue);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Value for '"
                                + fieldName
                                + "' must be an integer or a string representing an integer.");
            }
        }
        throw new IllegalArgumentException("Unsupported JSON type for field '" + fieldName + "'.");
    }

    private DiskSpec parseDiskSpec(JsonNode diskNode, DeserializationContext ctxt, JsonParser p)
            throws IOException {
        Boolean enabled = diskNode.has("enabled") ? diskNode.get("enabled").asBoolean() : null;
        String type = diskNode.has("type") ? diskNode.get("type").asText() : null;
        String size = diskNode.has("size") ? diskNode.get("size").asText() : null;
        return new DiskSpec(enabled, type, size);
    }
}
