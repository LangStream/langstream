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
package ai.langstream.deployer.k8s.agents;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
public class AgentResourceUnitConfiguration {

    private float cpuPerUnit = 0.5f;
    // MB
    private long memPerUnit = 512;
    private int instancePerUnit = 1;

    private int defaultCpuMemUnits = 1;
    private int defaultInstanceUnits = 1;

    private int maxCpuMemUnits = 8;
    private int maxInstanceUnits = 8;

    private int defaultMaxTotalResourceUnitsPerTenant = 0;

    private boolean enableLivenessProbe = true;
    private int livenessProbeInitialDelaySeconds = 10;
    private int livenessProbePeriodSeconds = 30;
    private int livenessProbeTimeoutSeconds = 5;

    private boolean enableReadinessProbe = true;
    private int readinessProbeInitialDelaySeconds = 10;
    private int readinessProbePeriodSeconds = 30;
    private int readinessProbeTimeoutSeconds = 5;

    private Map<String, String> storageClassesMapping = new HashMap<>();
    private String defaultStorageClass = "default";
    private String defaultStorageDiskSize = "128M";
}
