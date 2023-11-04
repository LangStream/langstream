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

public enum ComputeFieldType {
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

    /**
     * @deprecated Use TIMESTAMP, INSTANT or LOCAL_DATE_TIME instead to represent a date-time value.
     */
    @Deprecated
    DATETIME,
    BYTES,
    DECIMAL,
    ARRAY,
    MAP
}
