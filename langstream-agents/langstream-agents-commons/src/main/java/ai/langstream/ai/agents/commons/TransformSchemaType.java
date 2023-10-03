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
package ai.langstream.ai.agents.commons;

public enum TransformSchemaType {
    STRING,
    INT8,
    INT16,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    DATE,
    TIME,
    TIMESTAMP,
    INSTANT,
    LOCAL_DATE,
    LOCAL_TIME,
    LOCAL_DATE_TIME,
    BYTES,
    AVRO,
    JSON,
    PROTOBUF,
    MAP;

    public boolean isPrimitive() {
        return isPrimitiveType(this);
    }

    public boolean isStruct() {
        return isStructType(this);
    }

    public static boolean isPrimitiveType(TransformSchemaType type) {
        switch (type) {
            case STRING:
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case BYTES:
            case INSTANT:
            case LOCAL_DATE:
            case LOCAL_TIME:
            case LOCAL_DATE_TIME:
                return true;
            default:
                return false;
        }
    }

    public static boolean isStructType(TransformSchemaType type) {
        switch (type) {
            case AVRO:
            case JSON:
            case PROTOBUF:
                return true;
            default:
                return false;
        }
    }
}
