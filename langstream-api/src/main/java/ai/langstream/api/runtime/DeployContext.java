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
package ai.langstream.api.runtime;

import ai.langstream.api.webservice.application.ApplicationCodeInfo;

public interface DeployContext extends AutoCloseable {

    DeployContext NO_DEPLOY_CONTEXT = new NoOpDeployContext();

    class NoOpDeployContext implements DeployContext {

        @Override
        public ApplicationCodeInfo getApplicationCodeInfo(
                String tenant, String applicationId, String codeArchiveId) {
            return null;
        }

        @Override
        public boolean isAutoUpgradeRuntimeImage() {
            return false;
        }

        @Override
        public boolean isAutoUpgradeRuntimeImagePullPolicy() {
            return false;
        }

        @Override
        public boolean isAutoUpgradeAgentResources() {
            return false;
        }

        @Override
        public boolean isAutoUpgradeAgentPodTemplate() {
            return false;
        }

        @Override
        public long getApplicationSeed() {
            return -1L;
        }

        @Override
        public void close() {}
    }

    ApplicationCodeInfo getApplicationCodeInfo(
            String tenant, String applicationId, String codeArchiveId);

    boolean isAutoUpgradeRuntimeImage();

    boolean isAutoUpgradeRuntimeImagePullPolicy();

    boolean isAutoUpgradeAgentResources();

    boolean isAutoUpgradeAgentPodTemplate();

    long getApplicationSeed();

    @Override
    default void close() {}
}
