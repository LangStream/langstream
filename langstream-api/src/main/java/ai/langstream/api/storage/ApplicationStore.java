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
package ai.langstream.api.storage;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.Secrets;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.runtime.ExecutionPlan;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface ApplicationStore extends GenericStore {

    void validateTenant(String tenant, boolean failIfNotExists) throws IllegalStateException;

    void onTenantCreated(String tenant);

    void onTenantUpdated(String tenant);

    void onTenantDeleted(String tenant);

    void put(
            String tenant,
            String applicationId,
            Application applicationInstance,
            String codeArchiveReference,
            ExecutionPlan executionPlan,
            boolean autoUpgrade,
            boolean forceRestart);

    StoredApplication get(String tenant, String applicationId, boolean queryPods);

    ApplicationSpecs getSpecs(String tenant, String applicationId);

    Secrets getSecrets(String tenant, String applicationId);

    void delete(String tenant, String applicationId, boolean force);

    Map<String, StoredApplication> list(String tenant);

    Map<String, Integer> getResourceUsage(String tenant);

    String getExecutorServiceURI(String tenant, String applicationId, String executorId);

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    class LogOptions {
        private List<String> filterReplicas;
    }

    interface PodLogHandler {
        void start(LogLineConsumer onLogLine);

        String getPodName();

        void close();
    }

    interface LogLineConsumer {

        LogLineResult onPodNotRunning(String state, String reason);

        LogLineResult onLogLine(String content, long timestamp);

        LogLineResult onPodLogNotAvailable();

        void onEnd();
    }

    record LogLineResult(boolean continueLogging, Long delayInSeconds) {}

    List<PodLogHandler> logs(String tenant, String applicationId, LogOptions logOptions);
}
