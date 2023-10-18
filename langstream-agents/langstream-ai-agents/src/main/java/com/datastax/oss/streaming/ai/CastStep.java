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
package com.datastax.oss.streaming.ai;

import static ai.langstream.ai.agents.commons.MutableRecord.attemptJsonConversion;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.TransformSchemaType;
import ai.langstream.ai.agents.commons.jstl.JstlTypeConverter;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import lombok.Builder;

@Builder
public class CastStep implements TransformStep {
    private final TransformSchemaType keySchemaType;
    private final TransformSchemaType valueSchemaType;

    private final boolean attemptJsonConversion;

    @Override
    public void process(MutableRecord mutableRecord) {
        if (mutableRecord.getKeySchemaType() != null
                && keySchemaType != null
                && mutableRecord.getKeySchemaType() != keySchemaType) {
            Object value = convertValue(mutableRecord.getKeyObject(), keySchemaType);
            mutableRecord.setKeySchemaType(keySchemaType);
            mutableRecord.setKeyObject(value);
        }
        if (valueSchemaType != null && mutableRecord.getValueSchemaType() != valueSchemaType) {
            Object value = convertValue(mutableRecord.getValueObject(), valueSchemaType);
            mutableRecord.setValueSchemaType(valueSchemaType);
            mutableRecord.setValueObject(value);
        }
    }

    private Object convertValue(Object originalValue, TransformSchemaType schemaType) {
        Object result =
                JstlTypeConverter.INSTANCE.coerceToType(originalValue, getJavaType(schemaType));
        return attemptJsonConversion ? attemptJsonConversion(result) : result;
    }

    private Class<?> getJavaType(TransformSchemaType type) {
        switch (type) {
            case STRING:
                return String.class;
            case INT8:
                return Byte.class;
            case INT16:
                return Short.class;
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
                return Date.class;
            case TIMESTAMP:
                return Timestamp.class;
            case TIME:
                return Time.class;
            case LOCAL_DATE_TIME:
                return LocalDateTime.class;
            case LOCAL_DATE:
                return LocalDate.class;
            case LOCAL_TIME:
                return LocalTime.class;
            case INSTANT:
                return Instant.class;
            case BYTES:
                return byte[].class;
            default:
                throw new UnsupportedOperationException("Unsupported schema type: " + type);
        }
    }

    public static class CastStepBuilder {
        private TransformSchemaType keySchemaType;
        private TransformSchemaType valueSchemaType;

        public CastStepBuilder keySchemaType(TransformSchemaType keySchemaType) {
            if (keySchemaType != null && keySchemaType.isStruct()) {
                throw new IllegalArgumentException(
                        "Unsupported key schema-type for Cast: " + keySchemaType);
            }
            this.keySchemaType = keySchemaType;
            return this;
        }

        public CastStepBuilder valueSchemaType(TransformSchemaType valueSchemaType) {
            if (valueSchemaType != null && valueSchemaType.isStruct()) {
                throw new IllegalArgumentException(
                        "Unsupported value schema-type for Cast: " + valueSchemaType);
            }
            this.valueSchemaType = valueSchemaType;
            return this;
        }
    }
}
