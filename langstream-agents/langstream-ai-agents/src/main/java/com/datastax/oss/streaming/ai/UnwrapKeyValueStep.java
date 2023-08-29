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

public class UnwrapKeyValueStep implements TransformStep {

    private final boolean unwrapKey;

    public UnwrapKeyValueStep(boolean unwrapKey) {
        this.unwrapKey = unwrapKey;
    }

    @Override
    public void process(TransformContext transformContext) throws Exception {
        if (transformContext.getKeySchemaType() != null) {
            if (unwrapKey) {
                transformContext.setValueSchemaType(transformContext.getKeySchemaType());
                transformContext.setValueNativeSchema(transformContext.getKeyNativeSchema());
                transformContext.setValueObject(transformContext.getKeyObject());
            }
            transformContext.setKeySchemaType(null);
            transformContext.setKeyNativeSchema(null);
            transformContext.setKeyObject(null);
        }
    }
}
