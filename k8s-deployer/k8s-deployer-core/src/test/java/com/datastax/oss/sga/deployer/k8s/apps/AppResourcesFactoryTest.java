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
                  name: test-'app
                  namespace: default
                spec:
                    image: ubuntu
                    imagePullPolicy: Always
                    application: "{app: true}"
                    tenant: my-tenant
                    codeArchiveId: "iiii"
                """);


        assertEquals("""
                        ---
                        apiVersion: batch/v1
                        kind: Job
                        metadata:
                          labels:
                            app: sga-deployer
                            sga-application: test-'app
                            sga-scope: deploy
                          name: sga-runtime-deployer-test-'app
                          namespace: default
                          ownerReferences:
                          - apiVersion: sga.oss.datastax.com/v1alpha1
                            kind: Application
                            blockOwnerDeletion: true
                            controller: true
                            name: test-'app
                        spec:
                          backoffLimit: 1
                          template:
                            metadata:
                              labels:
                                app: sga-deployer
                                sga-application: test-'app
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
                                - "echo '{\\"applicationId\\":\\"test-'\\"'\\"'app\\",\\"tenant\\":\\"my-tenant\\",\\"application\\":\\"{app: true}\\",\\"codeStorageArchiveId\\":\\"iiii\\"}' > /app-config/config && echo '{}' > /cluster-runtime-config/config"
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
                              restartPolicy: Never
                              serviceAccount: my-tenant
                              volumes:
                              - emptyDir: {}
                                name: app-config
                              - name: app-secrets
                                secret:
                                  secretName: test-'app
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
                            sga-application: test-'app
                            sga-scope: delete
                          name: sga-runtime-deployer-cleanup-test-'app
                          namespace: default
                          ownerReferences:
                          - apiVersion: sga.oss.datastax.com/v1alpha1
                            kind: Application
                            blockOwnerDeletion: true
                            controller: true
                            name: test-'app
                        spec:
                          backoffLimit: 1
                          template:
                            metadata:
                              labels:
                                app: sga-deployer
                                sga-application: test-'app
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
                                - "echo '{\\"applicationId\\":\\"test-'\\"'\\"'app\\",\\"tenant\\":\\"my-tenant\\",\\"application\\":\\"{app: true}\\",\\"codeStorageArchiveId\\":\\"iiii\\"}' > /app-config/config && echo '{}' > /cluster-runtime-config/config"
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
                              restartPolicy: Never
                              serviceAccount: my-tenant
                              volumes:
                              - emptyDir: {}
                                name: app-config
                              - name: app-secrets
                                secret:
                                  secretName: test-'app
                              - emptyDir: {}
                                name: cluster-runtime-config
                        """,
                SerializationUtil.writeAsYaml(AppResourcesFactory.generateJob(resource, Map.of(), true)));


    }

    private ApplicationCustomResource getCr(String yaml) {
        return SerializationUtil.readYaml(yaml, ApplicationCustomResource.class);
    }
}
