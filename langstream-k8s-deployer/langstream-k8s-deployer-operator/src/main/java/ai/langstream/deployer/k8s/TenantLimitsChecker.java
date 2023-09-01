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
package ai.langstream.deployer.k8s;

import ai.langstream.api.webservice.tenant.TenantConfiguration;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.limits.ApplicationResourceLimitsChecker;
import ai.langstream.deployer.k8s.util.KeyedLockHandler;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.function.Function;
import lombok.AllArgsConstructor;

@ApplicationScoped
public class TenantLimitsChecker {

    private final ApplicationResourceLimitsChecker resourceLimitsEnforcer;
    private final LimitsSupplier limitsSupplier;

    public TenantLimitsChecker(
            ResolvedDeployerConfiguration resolvedDeployerConfiguration,
            KubernetesClient kubernetesClient,
            TenantsConfigurationReader tenantsConfigurationReader) {
        this.limitsSupplier =
                new LimitsSupplier(
                        resolvedDeployerConfiguration
                                .getAgentResources()
                                .getDefaultMaxTotalResourceUnitsPerTenant(),
                        tenantsConfigurationReader);
        this.resourceLimitsEnforcer =
                new ApplicationResourceLimitsChecker(
                        kubernetesClient, new KeyedLockHandler(), limitsSupplier);
    }

    LimitsSupplier getLimitsSupplier() {
        return limitsSupplier;
    }

    @AllArgsConstructor
    static class LimitsSupplier implements Function<String, Integer> {
        private final int defaultLimit;
        private final TenantsConfigurationReader tenantsConfigurationReader;

        @Override
        public Integer apply(String s) {
            final TenantConfiguration config = tenantsConfigurationReader.getTenantConfiguration(s);
            if (config == null) {
                return defaultLimit;
            }
            if (config.getMaxTotalResourceUnits() <= 0) {
                return defaultLimit;
            }
            return config.getMaxTotalResourceUnits();
        }
    }

    public boolean checkLimitsForTenant(ApplicationCustomResource applicationCustomResource) {
        return resourceLimitsEnforcer.checkLimitsForTenant(applicationCustomResource);
    }

    public void onAppBeingDeleted(ApplicationCustomResource applicationCustomResource) {
        resourceLimitsEnforcer.onAppBeingDeleted(applicationCustomResource);
    }
}
