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
package com.datastax.oss.sga.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TopicDefinition extends Connection.Connectable {

    public static final String CREATE_MODE_NONE = "none";
    public static final String CREATE_MODE_CREATE_IF_NOT_EXISTS = "create-if-not-exists";

    public TopicDefinition() {
        if (creationMode == null) {
            creationMode = CREATE_MODE_NONE;
        }
        connectableType = Connection.Connectables.TOPIC;
    }

    public TopicDefinition(String name, String creationMode, SchemaDefinition schema) {
        this();
        this.name = name;
        this.creationMode = creationMode;
        this.schema = schema;
        validateCreationMode();
    }

    private String name;

    @JsonProperty("creation-mode")
    private String creationMode = CREATE_MODE_NONE;
    private SchemaDefinition schema;

    private void validateCreationMode() {
        switch (creationMode) {
            case CREATE_MODE_NONE:
            case CREATE_MODE_CREATE_IF_NOT_EXISTS:
                break;
            default:
                throw new IllegalArgumentException("Invalid creation mode: " + creationMode);

        }
    }
}
