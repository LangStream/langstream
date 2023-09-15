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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class TopicDefinition {

    public static final String CREATE_MODE_NONE = "none";
    public static final String CREATE_MODE_CREATE_IF_NOT_EXISTS = "create-if-not-exists";

    public static final String DELETE_MODE_NONE = "none";
    public static final String DELETE_MODE_DELETE = "delete";

    public TopicDefinition() {
        creationMode = CREATE_MODE_NONE;
        deletionMode = DELETE_MODE_NONE;
    }

    public static TopicDefinition fromName(String name) {
        return new TopicDefinition(
                name, CREATE_MODE_NONE, DELETE_MODE_NONE, false, 0, null, null, Map.of(), Map.of());
    }

    public TopicDefinition(
            String name,
            String creationMode,
            String deletionMode,
            boolean implicit,
            int partitions,
            SchemaDefinition keySchema,
            SchemaDefinition valueSchema,
            Map<String, Object> options,
            Map<String, Object> config) {
        this();
        this.name = name;
        this.creationMode = Objects.requireNonNullElse(creationMode, CREATE_MODE_NONE);
        this.deletionMode = Objects.requireNonNullElse(deletionMode, DELETE_MODE_NONE);
        this.implicit = implicit;
        this.partitions = partitions;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.options = options;
        this.config = config;
        validateCreationMode();
    }

    private String name;

    @JsonProperty("creation-mode")
    private String creationMode;

    @JsonProperty("deletion-mode")
    private String deletionMode;

    // Kafka Admin special configuration options
    private Map<String, Object> config;
    private Map<String, Object> options;
    private SchemaDefinition keySchema;
    private SchemaDefinition valueSchema;
    private int partitions;

    /** If true, the topic is not declared in the application, but is expected to exist. */
    private boolean implicit;

    private void validateCreationMode() {
        switch (creationMode) {
            case CREATE_MODE_NONE:
            case CREATE_MODE_CREATE_IF_NOT_EXISTS:
                break;
            default:
                throw new IllegalArgumentException("Invalid creation mode: " + creationMode);
        }
    }

    private void validateDeletionMode() {
        switch (deletionMode) {
            case DELETE_MODE_DELETE:
            case DELETE_MODE_NONE:
                break;
            default:
                throw new IllegalArgumentException("Invalid deletion mode: " + deletionMode);
        }
    }

    public TopicDefinition copy() {
        TopicDefinition copy = new TopicDefinition();
        copy.setName(name);
        copy.setCreationMode(creationMode);
        copy.setDeletionMode(deletionMode);
        copy.setImplicit(implicit);
        copy.setPartitions(partitions);
        copy.setKeySchema(keySchema);
        copy.setValueSchema(valueSchema);
        copy.setOptions(options);
        copy.setConfig(config);
        return copy;
    }
}
