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
package ai.langstream.api.runner.assets;

import ai.langstream.api.model.AssetDefinition;

public record AssetManagerAndLoader(AssetManager agentCode, ClassLoader classLoader) {

    public void executeWithContextClassloader(RunnableWithException code) throws Exception {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            code.run(agentCode);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public <T> T callWithContextClassloader(CallableWithException code) throws Exception {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return (T) code.call(agentCode);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    public AssetManager asAssetManager() {

        return new AssetManager() {

            @Override
            public void initialize(AssetDefinition assetDefinition) throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.initialize(assetDefinition));
            }

            @Override
            public boolean assetExists() throws Exception {
                return callWithContextClassloader(agentCode -> agentCode.assetExists());
            }

            @Override
            public void deployAsset() throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.deployAsset());
            }

            @Override
            public boolean deleteAssetIfExists() throws Exception {
                return callWithContextClassloader(agentCode -> agentCode.deleteAssetIfExists());
            }

            @Override
            public void close() throws Exception {
                executeWithContextClassloader(agentCode -> agentCode.close());
            }
        };
    }

    public void close() throws Exception {
        executeWithContextClassloader(agentCode -> agentCode.close());
    }

    @FunctionalInterface
    public interface RunnableWithException {
        void run(AssetManager agent) throws Exception;
    }

    @FunctionalInterface
    public interface CallableWithException {
        Object call(AssetManager agent) throws Exception;
    }
}
