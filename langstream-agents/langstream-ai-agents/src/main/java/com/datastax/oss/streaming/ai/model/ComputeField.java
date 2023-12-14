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
package com.datastax.oss.streaming.ai.model;

import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import jakarta.el.ELException;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;

@Getter
public class ComputeField {
    /**
     * Full field name in the following format: "value.fieldName", "key.fieldName" or "headerName".
     */
    @Getter(AccessLevel.NONE)
    private final String scopedName;

    private final ComputeFieldType type;

    /** The name of the field without the prefix. */
    private String name;

    /**
     * The scope of the message where the computed field will be applied. Could be "key", "value" or
     * "header"
     */
    private String scope;

    private JstlEvaluator<?> evaluator;
    private final boolean optional;
    private static final Set<String> validComputeHeaders = Set.of("destinationTopic", "messageKey");

    @Builder
    private ComputeField(String scopedName, ComputeFieldType type, boolean optional) {
        this.scopedName = scopedName;
        this.type = type;
        this.optional = optional;
    }

    private ComputeField(
            String name,
            JstlEvaluator<?> evaluator,
            ComputeFieldType type,
            String scope,
            boolean optional) {
        this(name, type, optional);
        this.evaluator = evaluator;
        this.scope = scope;
        this.name = name;
    }

    public static class ComputeFieldBuilder {
        private String expression;
        private JstlEvaluator<?> evaluator;
        private String scope;
        private String name;

        public ComputeFieldBuilder expression(String expression) {
            this.expression = expression;
            return this;
        }

        public ComputeField build() {
            // Compile the jstl evaluator to validate the expression syntax early on.
            try {
                this.validateAndParseScopedName();
                this.evaluator =
                        new JstlEvaluator<>(String.format("${%s}", this.expression), getJavaType());
            } catch (ELException ex) {
                throw new IllegalArgumentException("invalid expression: " + expression, ex);
            }
            return new ComputeField(name, evaluator, type, scope, optional);
        }

        private void validateAndParseScopedName() {
            if (this.scopedName.equals("value")) {
                this.scope = "primitive";
                this.name = "value";
            } else if (this.scopedName.equals("key")) {
                this.scope = "primitive";
                this.name = "key";
            } else if (this.scopedName.startsWith("key.") || this.scopedName.startsWith("value.")) {
                String[] nameParts = this.scopedName.split("\\.", 2);
                this.scope = nameParts[0];
                this.name = nameParts[1];
            } else if (this.scopedName.startsWith("properties.")) {
                String[] nameParts = this.scopedName.split("\\.", 2);
                this.scope = "header.properties";
                this.name = nameParts[1];
            } else if (validComputeHeaders.contains(this.scopedName)) {
                this.scope = "header";
                this.name = this.scopedName;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid compute field name: %s. "
                                        + "It should be prefixed with 'key.' or 'value.' or 'properties.' or be one of "
                                        + "[key, value, destinationTopic, messageKey]",
                                this.scopedName));
            }
        }

        private Class<?> getJavaType() {
            if (this.type == null) {
                return Object.class;
            }
            switch (this.type) {
                case STRING:
                    return String.class;
                case INT8:
                    return Byte.class;
                case INT16:
                    return short.class;
                case INT32:
                    return Integer.class;
                case INT64:
                    return Long.class;
                case FLOAT:
                    return Float.class;
                case DOUBLE:
                    return Double.class;
                case BOOLEAN:
                    return Boolean.class;
                case DATE:
                case LOCAL_DATE:
                    return LocalDate.class;
                case TIME:
                    return Time.class;
                case LOCAL_TIME:
                    return LocalTime.class;
                case LOCAL_DATE_TIME:
                    return LocalDateTime.class;
                case DATETIME:
                case INSTANT:
                    return Instant.class;
                case TIMESTAMP:
                    return Timestamp.class;
                case BYTES:
                    return byte[].class;
                case DECIMAL:
                    return BigDecimal.class;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported compute field type: " + type);
            }
        }
    }
}
