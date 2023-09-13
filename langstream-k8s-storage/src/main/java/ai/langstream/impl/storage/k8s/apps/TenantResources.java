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
package ai.langstream.impl.storage.k8s.apps;

import static ai.langstream.impl.storage.k8s.apps.KubernetesApplicationStore.encodeSecret;

import ai.langstream.deployer.k8s.CRDConstants;
import ai.langstream.runtime.api.ClusterConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class TenantResources {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final KubernetesApplicationStoreProperties properties;
    private final KubernetesClient client;
    private final String tenant;
    private final String namespace;

    void ensureTenantResources() {
        if (properties.getControlPlaneUrl() == null || properties.getControlPlaneUrl().isBlank()) {
            throw new IllegalArgumentException("controlPlaneUrl is required");
        }
        ensureNamespace();
        ensureServiceAccount();
        ensureRole();
        ensureRoleBinding();
        ensureClusterConfiguration();
        ensureServiceAccountRuntime();
    }

    @SneakyThrows
    private void ensureClusterConfiguration() {
        final ClusterConfiguration clusterConfiguration =
                new ClusterConfiguration(properties.getControlPlaneUrl());
        final String encoded = encodeSecret(mapper.writeValueAsString(clusterConfiguration));
        final Secret secret =
                new SecretBuilder()
                        .withNewMetadata()
                        .withName(CRDConstants.TENANT_CLUSTER_CONFIG_SECRET)
                        .withNamespace(namespace)
                        .endMetadata()
                        .withData(Map.of(CRDConstants.TENANT_CLUSTER_CONFIG_SECRET_KEY, encoded))
                        .build();
        client.resource(secret).inNamespace(namespace).serverSideApply();

        log.info("Cluster configuration secret {} up to date", tenant);
    }

    private void ensureRoleBinding() {
        final String serviceAccount = CRDConstants.computeDeployerServiceAccountForTenant(tenant);
        client.resource(
                        new RoleBindingBuilder()
                                .withNewMetadata()
                                .withName(tenant)
                                .endMetadata()
                                .withNewRoleRef()
                                .withKind("Role")
                                .withApiGroup("rbac.authorization.k8s.io")
                                .withName(tenant)
                                .endRoleRef()
                                .withSubjects(
                                        new SubjectBuilder()
                                                .withName(serviceAccount)
                                                .withNamespace(namespace)
                                                .withKind("ServiceAccount")
                                                .build())
                                .build())
                .inNamespace(namespace)
                .serverSideApply();
        log.info("Role binding {} up to date", tenant);
    }

    private void ensureRole() {
        client.resource(
                        new RoleBuilder()
                                .withNewMetadata()
                                .withName(tenant)
                                .endMetadata()
                                .withRules(
                                        new PolicyRuleBuilder()
                                                .withApiGroups("langstream.ai")
                                                .withResources("agents")
                                                .withVerbs("*")
                                                .build(),
                                        new PolicyRuleBuilder()
                                                .withApiGroups("")
                                                .withResources("secrets")
                                                .withVerbs("*")
                                                .build())
                                .build())
                .inNamespace(namespace)
                .serverSideApply();

        log.info("Role {} up to date", tenant);
    }

    private void ensureServiceAccount() {
        final String name = CRDConstants.computeDeployerServiceAccountForTenant(tenant);
        client.resource(
                        new ServiceAccountBuilder()
                                .withNewMetadata()
                                .withName(name)
                                .endMetadata()
                                .build())
                .inNamespace(namespace)
                .serverSideApply();
        log.info("Service account {} up to date", name);
    }

    private void ensureServiceAccountRuntime() {
        final String name = CRDConstants.computeRuntimeServiceAccountForTenant(tenant);
        client.resource(
                        new ServiceAccountBuilder()
                                .withNewMetadata()
                                .withName(name)
                                .endMetadata()
                                .build())
                .inNamespace(namespace)
                .serverSideApply();
        log.info("Service account {} up to date", name);
    }

    private void ensureNamespace() {
        if (client.namespaces().withName(namespace).get() == null) {
            client.resource(
                            new NamespaceBuilder()
                                    .withNewMetadata()
                                    .withName(namespace)
                                    .endMetadata()
                                    .build())
                    .serverSideApply();
            log.info("Namespace {} up to date", namespace, tenant);
        }
    }
}
