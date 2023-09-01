package ai.langstream.deployer.k8s;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kubernetes.client.KubernetesTestServer;
import io.quarkus.test.kubernetes.client.WithKubernetesTestServer;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@WithKubernetesTestServer
@TestProfile(TenantLimitsCheckerTest.MyTestProfile.class)
class TenantLimitsCheckerTest {

    @KubernetesTestServer KubernetesServer mockServer;

    public static class MyTestProfile implements QuarkusTestProfile {

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "deployer.agent-resources",
                    "{\"defaultMaxTotalResourceUnitsPerTenant\":10}",
                    "deployer.global-storage",
                    "{\"type\":\"kubernetes\",\"configuration\":{\"namespace\":\"default\"}}");
        }
    }

    @Inject TenantLimitsChecker checker;

    @BeforeEach
    public void before() {
        mockServer
                .getKubernetesMockServer()
                .expect()
                .get()
                .withPath("/api/v1/namespaces/default/configmaps/langstream-t-my-tenant")
                .andReturn(
                        200,
                        new ConfigMapBuilder()
                                .withData(Map.of("value", "{\"maxTotalResourceUnits\":20}"))
                                .build())
                .always();

        mockServer
                .getKubernetesMockServer()
                .expect()
                .get()
                .withPath("/api/v1/namespaces/default/configmaps/langstream-t-my-tenant3")
                .andReturn(
                        200,
                        new ConfigMapBuilder()
                                .withData(Map.of("value", "{\"maxTotalResourceUnits\":0}"))
                                .build())
                .always();
    }

    @Test
    void testLimitsSupplier() {
        final TenantLimitsChecker.LimitsSupplier supplier = checker.getLimitsSupplier();
        Assertions.assertEquals(10, supplier.apply("my-tenant2"));
        Assertions.assertEquals(10, supplier.apply("my-tenant3"));
        Assertions.assertEquals(20, supplier.apply("my-tenant"));
    }
}
