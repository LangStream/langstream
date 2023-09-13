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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.langstream.deployer.k8s.CRDConstants;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.k8s.tests.KubeK3sServer;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class KubernetesTenantResourcesTest {

    @RegisterExtension static final KubeK3sServer k3s = new KubeK3sServer(true);

    @Test
    void testResources() {

        final KubernetesApplicationStore store = new KubernetesApplicationStore();
        store.initialize(
                Map.of(
                        "controlPlaneUrl",
                        "http://control-plane.localhost:8090",
                        "namespaceprefix",
                        "s"));

        final String tenant = "tenant-resources";
        store.onTenantCreated(tenant);
        final String namespace = "s" + tenant;
        assertResources(tenant, namespace);
        store.onTenantCreated(tenant);
        assertResources(tenant, namespace);
    }

    private void assertResources(String tenant, String namespace) {
        assertNotNull(k3s.getClient().namespaces().withName(namespace).get());
        Assertions.assertNotNull(
                k3s.getClient()
                        .resources(ServiceAccount.class)
                        .inNamespace(namespace)
                        .withName(tenant)
                        .get());

        Assertions.assertNotNull(
                k3s.getClient()
                        .resources(ServiceAccount.class)
                        .inNamespace(namespace)
                        .withName("runtime-" + tenant)
                        .get());
        Assertions.assertEquals(
                """
                ---
                - apiGroups:
                  - langstream.ai
                  resources:
                  - agents
                  verbs:
                  - '*'
                - apiGroups:
                  - ""
                  resources:
                  - secrets
                  verbs:
                  - '*'
                """,
                SerializationUtil.writeAsYaml(
                        k3s.getClient()
                                .resources(Role.class)
                                .inNamespace(namespace)
                                .withName(tenant)
                                .get()
                                .getRules()));

        Assertions.assertEquals(
                """
                ---
                kind: Role
                apiGroup: rbac.authorization.k8s.io
                name: tenant-resources
                """,
                SerializationUtil.writeAsYaml(
                        k3s.getClient()
                                .resources(RoleBinding.class)
                                .inNamespace(namespace)
                                .withName(tenant)
                                .get()
                                .getRoleRef()));

        Assertions.assertEquals(
                """
                ---
                - kind: ServiceAccount
                  name: tenant-resources
                  namespace: stenant-resources
                """,
                SerializationUtil.writeAsYaml(
                        k3s.getClient()
                                .resources(RoleBinding.class)
                                .inNamespace(namespace)
                                .withName(tenant)
                                .get()
                                .getSubjects()));

        final Secret secret =
                k3s.getClient()
                        .resources(Secret.class)
                        .inNamespace(namespace)
                        .withName(CRDConstants.TENANT_CLUSTER_CONFIG_SECRET)
                        .get();
        Assertions.assertEquals(
                Map.of(
                        "cluster-config",
                        "eyJjb250cm9sUGxhbmVVcmwiOiJodHRwOi8vY29udHJvbC1wbGFuZS5sb2NhbGhvc3Q6ODA5MCJ9"),
                secret.getData());
    }
}
