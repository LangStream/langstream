package com.datastax.oss.sga.deployer.k8s.apps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AppResourcesFactoryTest {

    @Test
    void testJob() {
        final ApplicationCustomResource resource = getCr("""
                apiVersion: sga.oss.datastax.com/v1alpha1
                kind: Application
                metadata:
                  name: test-app
                  namespace: default
                spec:
                    image: ubuntu
                    imagePullPolicy: Always
                    application: "{app: true}"
                    tenant: my-tenant
                """);


        assertEquals("""
                        ---
                        apiVersion: batch/v1
                        kind: Job
                        metadata:
                          labels:
                            app: sga-deployer
                            sga-application: test-app
                            sga-scope: deploy
                          name: sga-runtime-deployer-test-app
                          namespace: default
                        spec:
                          template:
                            metadata:
                              labels:
                                app: sga-deployer
                                sga-application: test-app
                                sga-scope: deploy
                            spec:
                              containers:
                              - args:
                                - deployer-runtime
                                - deploy
                                - /cluster-runtime-config/config
                                - /app-config/config
                                - /app-secrets/secrets
                                image: ubuntu
                                imagePullPolicy: Always
                                name: deployer
                                resources:
                                  requests:
                                    cpu: 100m
                                    memory: 128Mi
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /app-secrets
                                  name: app-secrets
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                              initContainers:
                              - args:
                                - "echo '{\\"applicationId\\":\\"test-app\\",\\"tenant\\":\\"my-tenant\\",\\"application\\":\\"{app: true}\\"}' > /app-config/config && echo '{}' > /cluster-runtime-config/config"
                                command:
                                - bash
                                - -c
                                image: ubuntu
                                imagePullPolicy: Always
                                name: deployer-init-config
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                              restartPolicy: OnFailure
                              serviceAccount: my-tenant
                              volumes:
                              - emptyDir: {}
                                name: app-config
                              - name: app-secrets
                                secret:
                                  secretName: test-app
                              - emptyDir: {}
                                name: cluster-runtime-config
                        """,
                SerializationUtil.writeAsYaml(AppResourcesFactory.generateJob(resource, Map.of(), false)));

        assertEquals("""
                        ---
                        apiVersion: batch/v1
                        kind: Job
                        metadata:
                          labels:
                            app: sga-deployer
                            sga-application: test-app
                            sga-scope: delete
                          name: sga-runtime-deployer-cleanup-test-app
                          namespace: default
                        spec:
                          template:
                            metadata:
                              labels:
                                app: sga-deployer
                                sga-application: test-app
                                sga-scope: delete
                            spec:
                              containers:
                              - args:
                                - deployer-runtime
                                - delete
                                - /cluster-runtime-config/config
                                - /app-config/config
                                - /app-secrets/secrets
                                image: ubuntu
                                imagePullPolicy: Always
                                name: deployer
                                resources:
                                  requests:
                                    cpu: 100m
                                    memory: 128Mi
                                terminationMessagePolicy: FallbackToLogsOnError
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /app-secrets
                                  name: app-secrets
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                              initContainers:
                              - args:
                                - "echo '{\\"applicationId\\":\\"test-app\\",\\"tenant\\":\\"my-tenant\\",\\"application\\":\\"{app: true}\\"}' > /app-config/config && echo '{}' > /cluster-runtime-config/config"
                                command:
                                - bash
                                - -c
                                image: ubuntu
                                imagePullPolicy: Always
                                name: deployer-init-config
                                volumeMounts:
                                - mountPath: /app-config
                                  name: app-config
                                - mountPath: /cluster-runtime-config
                                  name: cluster-runtime-config
                              restartPolicy: OnFailure
                              serviceAccount: my-tenant
                              volumes:
                              - emptyDir: {}
                                name: app-config
                              - name: app-secrets
                                secret:
                                  secretName: test-app
                              - emptyDir: {}
                                name: cluster-runtime-config
                        """,
                SerializationUtil.writeAsYaml(AppResourcesFactory.generateJob(resource, Map.of(), true)));


    }

    private ApplicationCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, ApplicationCustomResource.class);
    }
}