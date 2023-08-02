/**
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
package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.StoredApplication;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class ApplicationRuntimeInfo {

    private StoredApplication storedApplication;

    private List<RunnerInfo> runners = new ArrayList<>();

    public ApplicationRuntimeInfo(StoredApplication storedApplication) {
        this.storedApplication = storedApplication;
    }

    @Data
    public static class RunnerInfo {
        private String runnerName;
        private List<PodRuntimeInfo> instances = new ArrayList<>();
    }
    @Data
    public static class PodRuntimeInfo {
        private String podName;
        private Map<String, Object> info;
    }
}
