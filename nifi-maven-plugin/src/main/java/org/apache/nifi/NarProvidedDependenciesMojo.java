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
package org.apache.nifi;

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.handler.ArtifactHandler;
import org.apache.maven.artifact.handler.manager.ArtifactHandlerManager;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.DefaultProjectBuildingRequest;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.ProjectBuilder;
import org.apache.maven.project.ProjectBuildingException;
import org.apache.maven.project.ProjectBuildingRequest;
import org.apache.maven.shared.dependency.graph.DependencyGraphBuilder;
import org.apache.maven.shared.dependency.graph.DependencyGraphBuilderException;
import org.apache.maven.shared.dependency.graph.DependencyNode;
import org.apache.maven.shared.dependency.graph.traversal.DependencyNodeVisitor;
import org.apache.nifi.utils.NarDependencyUtils;
import org.eclipse.aether.RepositorySystemSession;

/**
 * Generates the listing of dependencies that is provided by the NAR dependency of the current NAR.
 * This is important as artifacts that bundle dependencies will not project those dependences using
 * the traditional maven dependency plugin. This plugin will override that setting in order to print
 * the dependencies being inherited at runtime.
 */
@Mojo(
        name = "provided-nar-dependencies",
        defaultPhase = LifecyclePhase.PACKAGE,
        threadSafe = true,
        requiresDependencyResolution = ResolutionScope.RUNTIME)
public class NarProvidedDependenciesMojo extends AbstractMojo {

    /** The Maven project. */
    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    /** The local artifact repository. */
    @Parameter(defaultValue = "${localRepository}", readonly = true)
    private ArtifactRepository localRepository;

    /**
     * The {@link RepositorySystemSession} used for obtaining the local and remote artifact
     * repositories.
     */
    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
    private RepositorySystemSession repoSession;

    /**
     * If specified, this parameter will cause the dependency tree to be written using the specified
     * format. Currently supported format are: <code>tree</code> or <code>pom</code>.
     */
    @Parameter(property = "mode", defaultValue = "tree")
    private String mode;

    /** The dependency tree builder to use for verbose output. */
    @Component private DependencyGraphBuilder dependencyGraphBuilder;

    /**
     * * The {@link ArtifactHandlerManager} into which any extension {@link ArtifactHandler}
     * instances should have been injected when the extensions were loaded.
     */
    @Component private ArtifactHandlerManager artifactHandlerManager;

    /**
     * The {@link ProjectBuilder} used to generate the {@link MavenProject} for the nar artifact the
     * dependency tree is being generated for.
     */
    @Component private ProjectBuilder projectBuilder;

    /*
     * @see org.apache.maven.plugin.Mojo#execute()
     */
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        try {
            NarDependencyUtils.ensureSingleNarDependencyExists(project);
            // build the project for the nar artifact
            final ProjectBuildingRequest narRequest = new DefaultProjectBuildingRequest();
            narRequest.setRepositorySession(repoSession);
            narRequest.setSystemProperties(System.getProperties());

            artifactHandlerManager.addHandlers(
                    NarDependencyUtils.createNarHandlerMap(narRequest, project, projectBuilder));

            // get the dependency tree
            final DependencyNode root =
                    dependencyGraphBuilder.buildDependencyGraph(narRequest, null);

            // write the appropriate output
            DependencyNodeVisitor visitor = null;
            if ("tree".equals(mode)) {
                visitor = new TreeWriter();
            } else if ("pom".equals(mode)) {
                visitor = new PomWriter();
            }

            // ensure the mode was specified correctly
            if (visitor == null) {
                throw new MojoExecutionException(
                        "The specified mode is invalid. Supported options are 'tree' and 'pom'.");
            }

            // visit and print the results
            root.accept(visitor);
            getLog().info(
                            "--- Provided NAR Dependencies ---"
                                    + System.lineSeparator()
                                    + System.lineSeparator()
                                    + visitor);
        } catch (ProjectBuildingException | DependencyGraphBuilderException e) {
            throw new MojoExecutionException("Cannot build project dependency tree", e);
        }
    }

    /**
     * Gets the Maven project used by this mojo.
     *
     * @return the Maven project
     */
    public MavenProject getProject() {
        return project;
    }

    /**
     * Returns whether the specified dependency has test scope.
     *
     * @param node The dependency
     * @return What the dependency is a test scoped dep
     */
    private boolean isTest(final DependencyNode node) {
        return "test".equals(node.getArtifact().getScope());
    }

    /** A dependency visitor that builds a dependency tree. */
    private class TreeWriter implements DependencyNodeVisitor {

        private final StringBuilder output = new StringBuilder();
        private final Deque<DependencyNode> hierarchy = new ArrayDeque<>();

        @Override
        public boolean visit(DependencyNode node) {
            // add this node
            hierarchy.push(node);

            // don't print test deps, but still add to hierarchy as they will
            // be removed in endVisit below
            if (isTest(node)) {
                return false;
            }

            // build the padding
            final StringBuilder pad = new StringBuilder();
            for (int i = 0; i < hierarchy.size() - 1; i++) {
                pad.append("   ");
            }
            pad.append("+- ");

            // log it
            output.append(pad).append(node.toNodeString()).append(System.lineSeparator());

            return true;
        }

        @Override
        public boolean endVisit(DependencyNode node) {
            hierarchy.pop();
            return true;
        }

        @Override
        public String toString() {
            return output.toString();
        }
    }

    /**
     * A dependency visitor that generates output that can be copied into a pom's dependency
     * management section.
     */
    private class PomWriter implements DependencyNodeVisitor {

        private final StringBuilder output = new StringBuilder();

        @Override
        public boolean visit(DependencyNode node) {
            if (isTest(node)) {
                return false;
            }

            final Artifact artifact = node.getArtifact();
            if (!NarDependencyUtils.NAR.equals(artifact.getType())) {
                output.append("<dependency>").append(System.lineSeparator());
                output.append("    <groupId>")
                        .append(artifact.getGroupId())
                        .append("</groupId>")
                        .append(System.lineSeparator());
                output.append("    <artifactId>")
                        .append(artifact.getArtifactId())
                        .append("</artifactId>")
                        .append(System.lineSeparator());
                output.append("    <version>")
                        .append(artifact.getVersion())
                        .append("</version>")
                        .append(System.lineSeparator());
                output.append("    <scope>provided</scope>").append(System.lineSeparator());
                output.append("</dependency>").append(System.lineSeparator());
            }

            return true;
        }

        @Override
        public boolean endVisit(DependencyNode node) {
            return true;
        }

        @Override
        public String toString() {
            return output.toString();
        }
    }
}
