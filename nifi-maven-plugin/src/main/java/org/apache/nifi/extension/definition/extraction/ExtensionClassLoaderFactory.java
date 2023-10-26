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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.ArtifactHandler;
import org.apache.maven.artifact.handler.manager.ArtifactHandlerManager;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.resolver.ArtifactResolutionRequest;
import org.apache.maven.artifact.resolver.ArtifactResolutionResult;
import org.apache.maven.artifact.resolver.ArtifactResolver;
import org.apache.maven.artifact.resolver.filter.ArtifactFilter;
import org.apache.maven.artifact.resolver.filter.ExclusionSetFilter;
import org.apache.maven.artifact.versioning.VersionRange;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.project.DefaultProjectBuildingRequest;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.ProjectBuilder;
import org.apache.maven.project.ProjectBuildingException;
import org.apache.maven.project.ProjectBuildingRequest;
import org.apache.maven.project.ProjectBuildingResult;
import org.apache.maven.shared.dependency.graph.DependencyGraphBuilder;
import org.apache.maven.shared.dependency.graph.DependencyGraphBuilderException;
import org.apache.maven.shared.dependency.graph.DependencyNode;
import org.apache.maven.shared.dependency.graph.traversal.DependencyNodeVisitor;
import org.eclipse.aether.RepositorySystemSession;

public class ExtensionClassLoaderFactory {

    private static final Set<String> EXCLUDED_ARTIFACT_IDS;

    static {
        final Set<String> excludedArtifactIds = new HashSet<>();
        excludedArtifactIds.add("jdk.tools:jdk.tools");
        excludedArtifactIds.add("com.sun:tools");
        EXCLUDED_ARTIFACT_IDS = Collections.unmodifiableSet(excludedArtifactIds);
    }

    private final Log log;
    private final MavenProject project;
    private final RepositorySystemSession repoSession;
    private final ProjectBuilder projectBuilder;
    private final ArtifactRepository localRepo;
    private final List<ArtifactRepository> remoteRepos;
    private final DependencyGraphBuilder dependencyGraphBuilder;
    private final ArtifactResolver artifactResolver;
    private final ArtifactHandlerManager artifactHandlerManager;

    private ExtensionClassLoaderFactory(final Builder builder) {
        this.log = builder.log;
        this.project = builder.project;
        this.repoSession = builder.repositorySession;
        this.projectBuilder = builder.projectBuilder;
        this.localRepo = builder.localRepo;
        this.remoteRepos = new ArrayList<>(builder.remoteRepos);
        this.dependencyGraphBuilder = builder.dependencyGraphBuilder;
        this.artifactResolver = builder.artifactResolver;
        this.artifactHandlerManager = builder.artifactHandlerManager;
    }

    private Log getLog() {
        return log;
    }

    public ExtensionClassLoader createExtensionClassLoader()
            throws MojoExecutionException, ProjectBuildingException {
        final Artifact narArtifact = project.getArtifact();
        final Set<Artifact> narArtifacts = getNarDependencies(narArtifact);

        final ArtifactsHolder artifactsHolder = new ArtifactsHolder();
        artifactsHolder.addArtifacts(narArtifacts);

        getLog().debug("Project artifacts: ");
        narArtifacts.forEach(artifact -> getLog().debug(artifact.getArtifactId()));

        final ExtensionClassLoader parentClassLoader =
                createClassLoader(narArtifacts, artifactsHolder);
        final ExtensionClassLoader classLoader =
                createClassLoader(narArtifacts, parentClassLoader, narArtifact);

        if (getLog().isDebugEnabled()) {
            getLog().debug("Full ClassLoader is:\n" + classLoader.toTree());
        }

        return classLoader;
    }

    private ExtensionClassLoader createClassLoader(
            final Set<Artifact> artifacts, final ArtifactsHolder artifactsHolder)
            throws MojoExecutionException, ProjectBuildingException {

        final Artifact nar = removeNarArtifact(artifacts);
        if (nar == null) {
            return createProvidedEntitiesClassLoader(artifactsHolder);
        }

        final Set<Artifact> narDependencies = getNarDependencies(nar);
        artifactsHolder.addArtifacts(narDependencies);

        return createClassLoader(
                narDependencies, createClassLoader(narDependencies, artifactsHolder), nar);
    }

    private Artifact removeNarArtifact(final Set<Artifact> artifacts) {
        final Iterator<Artifact> itr = artifacts.iterator();
        while (itr.hasNext()) {
            final Artifact artifact = itr.next();

            if (artifact.equals(project.getArtifact())) {
                continue;
            }

            if ("nar".equalsIgnoreCase(artifact.getType())) {
                getLog().info("Found NAR dependency of " + artifact);
                itr.remove();

                return artifact;
            }
        }

        return null;
    }

    private Set<Artifact> getNarDependencies(final Artifact narArtifact)
            throws MojoExecutionException, ProjectBuildingException {
        final ProjectBuildingRequest narRequest = createProjectBuildingRequest();

        final ProjectBuildingResult narResult = projectBuilder.build(narArtifact, narRequest);

        final Set<Artifact> narDependencies = gatherArtifacts(narResult.getProject(), TreeSet::new);
        narDependencies.remove(narArtifact);
        narDependencies.remove(project.getArtifact());

        getLog().debug(
                        "Found NAR dependency of "
                                + narArtifact
                                + ", which resolved to the following artifacts: "
                                + narDependencies);
        return narDependencies;
    }

    private String determineProvidedEntityVersion(
            final Set<Artifact> artifacts, final String groupId, final String artifactId)
            throws ProjectBuildingException, MojoExecutionException {
        getLog().debug("Determining provided entities for " + groupId + ":" + artifactId);

        for (final Artifact artifact : artifacts) {
            if (artifact.getGroupId().equals(groupId)
                    && artifact.getArtifactId().equals(artifactId)) {
                return artifact.getVersion();
            }
        }

        return findProvidedDependencyVersion(artifacts, groupId, artifactId);
    }

    private String findProvidedDependencyVersion(
            final Set<Artifact> artifacts, final String groupId, final String artifactId) {
        final ProjectBuildingRequest projectRequest = createProjectBuildingRequest();

        for (final Artifact artifact : artifacts) {
            try {
                final ProjectBuildingResult projectResult =
                        projectBuilder.build(artifact, projectRequest);
                final Set<Artifact> artifactDependencies =
                        gatherArtifacts(projectResult.getProject(), HashSet::new);
                getLog().debug("For Artifact " + artifact + ", found the following dependencies:");
                artifactDependencies.forEach(dep -> getLog().debug(dep.toString()));

                for (final Artifact dependency : artifactDependencies) {
                    if (dependency.getGroupId().equals(groupId)
                            && dependency.getArtifactId().equals(artifactId)) {
                        getLog().debug(
                                        "Found version of "
                                                + groupId
                                                + ":"
                                                + artifactId
                                                + " to be "
                                                + dependency.getVersion());
                        return dependency.getVersion();
                    }
                }
            } catch (final Exception e) {
                getLog().warn(
                                "Unable to construct Maven Project for "
                                        + artifact
                                        + " when attempting to determine the expected version of NiFi API");
                getLog().debug(
                                "Unable to construct Maven Project for "
                                        + artifact
                                        + " when attempting to determine the expected version of NiFi API",
                                e);
            }
        }

        return null;
    }

    private Artifact getProvidedArtifact(
            final String groupId, final String artifactId, final String version)
            throws MojoExecutionException {
        final ArtifactHandler handler = artifactHandlerManager.getArtifactHandler("jar");

        final VersionRange versionRange;
        try {
            versionRange = VersionRange.createFromVersionSpec(version);
        } catch (final Exception e) {
            throw new MojoExecutionException(
                    "Could not determine appropriate version for Provided Artifact "
                            + groupId
                            + ":"
                            + artifactId,
                    e);
        }

        final Artifact artifact =
                new DefaultArtifact(groupId, artifactId, versionRange, null, "jar", null, handler);

        final ArtifactResolutionRequest request = new ArtifactResolutionRequest();
        request.setLocalRepository(localRepo);
        request.setRemoteRepositories(remoteRepos);
        request.setArtifact(artifact);

        final ArtifactResolutionResult result = artifactResolver.resolve(request);
        if (!result.isSuccess()) {
            final List<Exception> exceptions = result.getExceptions();

            final MojoExecutionException exception =
                    new MojoExecutionException("Could not resolve local dependency " + artifact);
            if (exceptions != null) {
                for (final Exception e : exceptions) {
                    exception.addSuppressed(e);
                }
            }

            throw exception;
        }

        final Set<Artifact> artifacts = result.getArtifacts();
        if (artifacts.isEmpty()) {
            throw new MojoExecutionException(
                    "Could not resolve any artifacts for dependency " + artifact);
        }

        final List<Artifact> sorted = new ArrayList<>(artifacts);
        Collections.sort(sorted);

        return sorted.get(0);
    }

    private ExtensionClassLoader createProvidedEntitiesClassLoader(
            final ArtifactsHolder artifactsHolder)
            throws MojoExecutionException, ProjectBuildingException {

        final String nifiApiVersion =
                determineProvidedEntityVersion(
                        artifactsHolder.getAllArtifacts(), "org.apache.nifi", "nifi-api");
        if (nifiApiVersion == null) {
            throw new MojoExecutionException(
                    "Could not find any dependency, provided or otherwise, on [org.apache.nifi:nifi-api]");
        } else {
            getLog().info("Found a dependency on version " + nifiApiVersion + " of NiFi API");
        }

        final String slf4jApiVersion =
                determineProvidedEntityVersion(
                        artifactsHolder.getAllArtifacts(), "org.slf4j", "slf4j-api");

        final Artifact nifiApiArtifact =
                getProvidedArtifact("org.apache.nifi", "nifi-api", nifiApiVersion);
        final Artifact nifiFrameworkApiArtifact =
                getProvidedArtifact(
                        "org.apache.nifi", "nifi-framework-api", nifiApiArtifact.getVersion());

        final Artifact slf4jArtifact =
                getProvidedArtifact("org.slf4j", "slf4j-api", slf4jApiVersion);

        final Set<Artifact> providedArtifacts = new HashSet<>();
        providedArtifacts.add(nifiApiArtifact);
        providedArtifacts.add(nifiFrameworkApiArtifact);
        providedArtifacts.add(slf4jArtifact);

        getLog().debug(
                        "Creating Provided Entities Class Loader with artifacts: "
                                + providedArtifacts);
        return createClassLoader(providedArtifacts, null, null);
    }

    /* package visible for testing reasons */
    ExtensionClassLoader createClassLoader(
            final Set<Artifact> artifacts,
            final ExtensionClassLoader parent,
            final Artifact narArtifact)
            throws MojoExecutionException {
        final Set<URL> urls = new HashSet<>();
        for (final Artifact artifact : artifacts) {
            final Set<URL> artifactUrls = toURLs(artifact);
            urls.addAll(artifactUrls);
        }

        getLog().debug("Creating class loader with following dependencies: " + urls);

        final URL[] urlArray = urls.toArray(new URL[0]);
        if (parent == null) {
            return new ExtensionClassLoader(urlArray, narArtifact, artifacts);
        } else {
            return new ExtensionClassLoader(urlArray, parent, narArtifact, artifacts);
        }
    }

    private Set<Artifact> gatherArtifacts(
            final MavenProject mavenProject, final Supplier<Set<Artifact>> setSupplier)
            throws MojoExecutionException {
        final Set<Artifact> artifacts = setSupplier.get();
        final DependencyNodeVisitor nodeVisitor =
                new DependencyNodeVisitor() {
                    @Override
                    public boolean visit(final DependencyNode dependencyNode) {
                        final Artifact artifact = dependencyNode.getArtifact();
                        artifacts.add(artifact);
                        return true;
                    }

                    @Override
                    public boolean endVisit(final DependencyNode dependencyNode) {
                        return true;
                    }
                };

        try {
            final ProjectBuildingRequest projectRequest = createProjectBuildingRequest();
            projectRequest.setProject(mavenProject);

            final ArtifactFilter excludesFilter = new ExclusionSetFilter(EXCLUDED_ARTIFACT_IDS);
            final DependencyNode depNode =
                    dependencyGraphBuilder.buildDependencyGraph(projectRequest, excludesFilter);
            depNode.accept(nodeVisitor);
        } catch (DependencyGraphBuilderException e) {
            throw new MojoExecutionException("Failed to build dependency tree", e);
        }
        return artifacts;
    }

    private ProjectBuildingRequest createProjectBuildingRequest() {
        final ProjectBuildingRequest projectRequest = new DefaultProjectBuildingRequest();
        projectRequest.setRepositorySession(repoSession);
        projectRequest.setSystemProperties(System.getProperties());
        projectRequest.setUserProperties(System.getProperties());
        projectRequest.setLocalRepository(localRepo);
        projectRequest.setRemoteRepositories(remoteRepos);
        return projectRequest;
    }

    private Set<URL> toURLs(final Artifact artifact) throws MojoExecutionException {
        final Set<URL> urls = new HashSet<>();

        final File artifactFile = artifact.getFile();
        if (artifactFile == null) {
            getLog().debug(
                            "Attempting to resolve Artifact "
                                    + artifact
                                    + " because it has no File associated with it");

            final ArtifactResolutionRequest request = new ArtifactResolutionRequest();
            request.setLocalRepository(localRepo);
            request.setRemoteRepositories(remoteRepos);
            request.setArtifact(artifact);

            final ArtifactResolutionResult result = artifactResolver.resolve(request);
            if (!result.isSuccess()) {
                throw new MojoExecutionException("Could not resolve local dependency " + artifact);
            }

            getLog().debug("Resolved Artifact " + artifact + " to " + result.getArtifacts());

            for (final Artifact resolved : result.getArtifacts()) {
                urls.addAll(toURLs(resolved));
            }
        } else {
            try {
                final URL url = artifact.getFile().toURI().toURL();
                getLog().debug("Adding URL " + url + " to ClassLoader");
                urls.add(url);
            } catch (final MalformedURLException mue) {
                throw new MojoExecutionException(
                        "Failed to convert File " + artifact.getFile() + " into URL", mue);
            }
        }

        return urls;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Log log;
        private MavenProject project;
        private ArtifactRepository localRepo;
        private List<ArtifactRepository> remoteRepos;
        private DependencyGraphBuilder dependencyGraphBuilder;
        private ArtifactResolver artifactResolver;
        private ProjectBuilder projectBuilder;
        private RepositorySystemSession repositorySession;
        private ArtifactHandlerManager artifactHandlerManager;

        public Builder log(final Log log) {
            this.log = log;
            return this;
        }

        public Builder projectBuilder(final ProjectBuilder projectBuilder) {
            this.projectBuilder = projectBuilder;
            return this;
        }

        public Builder project(final MavenProject project) {
            this.project = project;
            return this;
        }

        public Builder localRepository(final ArtifactRepository localRepo) {
            this.localRepo = localRepo;
            return this;
        }

        public Builder remoteRepositories(final List<ArtifactRepository> remoteRepos) {
            this.remoteRepos = remoteRepos;
            return this;
        }

        public Builder dependencyGraphBuilder(final DependencyGraphBuilder dependencyGraphBuilder) {
            this.dependencyGraphBuilder = dependencyGraphBuilder;
            return this;
        }

        public Builder artifactResolver(final ArtifactResolver resolver) {
            this.artifactResolver = resolver;
            return this;
        }

        public Builder repositorySession(final RepositorySystemSession repositorySession) {
            this.repositorySession = repositorySession;
            return this;
        }

        public Builder artifactHandlerManager(final ArtifactHandlerManager artifactHandlerManager) {
            this.artifactHandlerManager = artifactHandlerManager;
            return this;
        }

        public ExtensionClassLoaderFactory build() {
            return new ExtensionClassLoaderFactory(this);
        }
    }

    private static class ArtifactsHolder {

        private Set<Artifact> allArtifacts = new TreeSet<>();

        public void addArtifacts(final Set<Artifact> artifacts) {
            if (artifacts != null) {
                allArtifacts.addAll(artifacts);
            }
        }

        public Set<Artifact> getAllArtifacts() {
            return allArtifacts;
        }
    }
}
