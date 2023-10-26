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
package org.apache.nifi.extension.definition.extraction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.ArtifactHandler;
import org.apache.maven.artifact.handler.manager.ArtifactHandlerManager;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.resolver.ArtifactResolutionRequest;
import org.apache.maven.artifact.resolver.ArtifactResolutionResult;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.ProjectBuilder;
import org.apache.maven.shared.dependency.graph.DependencyGraphBuilder;
import org.eclipse.aether.RepositorySystemSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExtensionClassLoaderFactoryTest {

    @Mock private Log log;
    @Mock private ArtifactResolver artifactResolver;
    @Mock private ArtifactRepository localRepository;
    @Mock private ArtifactRepository remoteRepository;
    @Mock private ArtifactHandlerManager artifactHandlerManager;
    @Mock private DependencyGraphBuilder dependencyGraphBuilder;
    @Mock private MavenProject project;
    @Mock private ProjectBuilder projectBuilder;
    @Mock private RepositorySystemSession repositorySession;
    private Artifact artifact1;
    private Artifact artifact2;
    private Artifact artifact3;

    // Test Subject
    private ExtensionClassLoaderFactory factory;

    @BeforeEach
    void setUp() {
        artifact1 = projectArtifact();
        artifact2 = localRepositoryDependencyArtifact();
        artifact3 = remoteRepositoryDependencyArtifact();

        when(artifactResolver.resolve(any(ArtifactResolutionRequest.class)))
                .thenAnswer(
                        args ->
                                resolved(
                                        args.getArgument(0, ArtifactResolutionRequest.class)
                                                .getArtifact()));

        factory =
                ExtensionClassLoaderFactory.builder()
                        .log(log)
                        .project(project)
                        .projectBuilder(projectBuilder)
                        .dependencyGraphBuilder(dependencyGraphBuilder)
                        .artifactHandlerManager(artifactHandlerManager)
                        .artifactResolver(artifactResolver)
                        .localRepository(localRepository)
                        .remoteRepositories(Collections.singletonList(remoteRepository))
                        .repositorySession(repositorySession)
                        .build();
    }

    @Test
    void createClassLoaderTest() throws Exception {
        Set<Artifact> dependencyArtifacts = new TreeSet<>();
        dependencyArtifacts.add(localRepositoryDependencyArtifact());
        dependencyArtifacts.add(remoteRepositoryDependencyArtifact());

        ExtensionClassLoader classLoader =
                factory.createClassLoader(dependencyArtifacts, null, artifact1);

        String[] expectedURLs = new String[] {"/path/to/service-api-nar", "/path/to/service-nar"};
        assertEquals(expectedURLs.length, classLoader.getURLs().length);
        List<String> expectedUrlsList = Arrays.asList(expectedURLs);
        List<String> actualUrlsList =
                Arrays.stream(classLoader.getURLs()).map(URL::getFile).collect(Collectors.toList());
        assertTrue(expectedUrlsList.containsAll(actualUrlsList));

        InOrder inOrder = inOrder(artifactResolver);
        for (ArtifactResolutionRequest req : getExpectedArtifactResolutionRequests()) {
            inOrder.verify(artifactResolver)
                    .resolve(
                            argThat(
                                    arg ->
                                            req.getArtifact()
                                                            .getArtifactId()
                                                            .equals(
                                                                    arg.getArtifact()
                                                                            .getArtifactId())
                                                    && req.getLocalRepository()
                                                            == arg.getLocalRepository()
                                                    && req.getRemoteRepositories()
                                                            .equals(arg.getRemoteRepositories())));
        }
        verifyNoMoreInteractions(artifactResolver);
    }

    private List<ArtifactResolutionRequest> getExpectedArtifactResolutionRequests() {
        ArtifactResolutionRequest request1 = new ArtifactResolutionRequest();
        request1.setArtifact(artifact2);
        request1.setLocalRepository(localRepository);
        request1.setRemoteRepositories(Collections.singletonList(remoteRepository));

        ArtifactResolutionRequest request2 = new ArtifactResolutionRequest();
        request2.setArtifact(artifact3);
        request2.setLocalRepository(localRepository);
        request2.setRemoteRepositories(Collections.singletonList(remoteRepository));

        List<ArtifactResolutionRequest> resolutionRequests = new ArrayList<>();
        resolutionRequests.add(request1);
        resolutionRequests.add(request2);
        return resolutionRequests;
    }

    private Artifact projectArtifact() {
        Artifact artifact =
                new DefaultArtifact(
                        "org.apache.nifi",
                        "processor-nar",
                        "1.0.0",
                        "compile",
                        "nar",
                        "arbitrary",
                        mock(ArtifactHandler.class));
        artifact.setFile(new File("/path/to/" + artifact.getArtifactId()));
        return artifact;
    }

    private Artifact localRepositoryDependencyArtifact() {
        Artifact artifact =
                new DefaultArtifact(
                        "org.apache.nifi",
                        "service-api-nar",
                        "1.0.0",
                        "compile",
                        "nar",
                        "arbitrary",
                        mock(ArtifactHandler.class));
        artifact.setFile(null);
        artifact.setRepository(localRepository);
        return artifact;
    }

    private Artifact remoteRepositoryDependencyArtifact() {
        Artifact artifact =
                new DefaultArtifact(
                        "org.apache.nifi",
                        "service-nar",
                        "1.0.0",
                        "provided",
                        "nar",
                        "arbitrary",
                        mock(ArtifactHandler.class));
        artifact.setFile(null);
        artifact.setRepository(remoteRepository);
        return artifact;
    }

    private ArtifactResolutionResult resolved(Artifact artifact) {
        Artifact resolvedArtifact =
                new DefaultArtifact(
                        artifact.getGroupId(),
                        artifact.getArtifactId(),
                        artifact.getVersion(),
                        artifact.getScope(),
                        artifact.getType(),
                        artifact.getClassifier(),
                        artifact.getArtifactHandler());
        resolvedArtifact.setFile(new File("/path/to/" + artifact.getArtifactId()));
        ArtifactResolutionResult result = new ArtifactResolutionResult();
        result.setArtifacts(Collections.singleton(resolvedArtifact));
        return result;
    }
}
