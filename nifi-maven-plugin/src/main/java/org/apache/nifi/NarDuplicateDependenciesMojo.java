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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.handler.ArtifactHandler;
import org.apache.maven.artifact.handler.manager.ArtifactHandlerManager;
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
import org.apache.maven.shared.dependency.graph.DependencyCollectorBuilder;
import org.apache.maven.shared.dependency.graph.DependencyCollectorBuilderException;
import org.apache.maven.shared.dependency.graph.DependencyNode;
import org.apache.maven.shared.dependency.graph.traversal.DependencyNodeVisitor;
import org.apache.nifi.utils.NarDependencyUtils;
import org.eclipse.aether.RepositorySystemSession;

/** Generates a list of duplicate dependencies with compile scope in the nar. */
@Mojo(
        name = "duplicate-nar-dependencies",
        defaultPhase = LifecyclePhase.PACKAGE,
        threadSafe = true,
        requiresDependencyResolution = ResolutionScope.RUNTIME)
public class NarDuplicateDependenciesMojo extends AbstractMojo {

    /** The Maven project. */
    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    /**
     * The {@link RepositorySystemSession} used for obtaining the local and remote artifact
     * repositories.
     */
    @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
    private RepositorySystemSession repoSession;

    /** The dependency tree builder to use for verbose output. */
    @Component private DependencyCollectorBuilder dependencyCollectorBuilder;

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
                    dependencyCollectorBuilder.collectDependencyGraph(narRequest, null);

            DependencyNode narParent =
                    root.getChildren().stream()
                            .filter(
                                    child ->
                                            NarDependencyUtils.NAR.equals(
                                                    child.getArtifact().getType()))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new MojoExecutionException(
                                                    "Project does not have any NAR dependencies."));

            getLog().info(
                            "Analyzing dependencies of "
                                    + narRequest.getProject().getFile().getPath());

            // all compiled dependencies except inherited from parent
            Map<String, List<Artifact>> directDependencies = new HashMap<>();

            root.accept(
                    new DependencyNodeVisitor() {
                        final Stack<Artifact> hierarchy = new Stack<>();

                        @Override
                        public boolean visit(DependencyNode node) {
                            if (node == root) {
                                return true;
                            }
                            Artifact artifact = node.getArtifact();
                            hierarchy.push(artifact);
                            if (NarDependencyUtils.COMPILE_STRING.equals(artifact.getScope())
                                    && !NarDependencyUtils.NAR.equals(artifact.getType())) {
                                directDependencies.put(
                                        artifact.toString(), new ArrayList<>(hierarchy));
                                return true;
                            }
                            return false;
                        }

                        @Override
                        public boolean endVisit(DependencyNode node) {
                            if (node != root) {
                                hierarchy.pop();
                            }
                            return true;
                        }
                    });

            Map<String, List<String>> errors = new HashMap<>();

            narParent.accept(
                    new DependencyNodeVisitor() {
                        final Stack<Artifact> hierarchy = new Stack<>();

                        @Override
                        public boolean visit(DependencyNode node) {
                            Artifact artifact = node.getArtifact();
                            hierarchy.push(artifact);
                            if (NarDependencyUtils.COMPILE_STRING.equals(artifact.getScope())
                                    && directDependencies.containsKey(artifact.toString())) {
                                StringBuilder sb =
                                        new StringBuilder()
                                                .append(root.getArtifact())
                                                .append(" (this nar)")
                                                .append(System.lineSeparator());
                                List<Artifact> otherHierarchy =
                                        directDependencies.get(artifact.toString());
                                // print other hierarchy
                                for (int i = 0; i < otherHierarchy.size(); i++) {
                                    sb.append(indent(i)).append(otherHierarchy.get(i));
                                    // print the last artifact in the hierarchy
                                    if (i == otherHierarchy.size() - 1) {
                                        sb.append(" (duplicate)");
                                    }
                                    sb.append(System.lineSeparator());
                                }
                                // print this hierarchy
                                for (int i = 0; i < hierarchy.size(); i++) {
                                    sb.append(indent(i)).append(hierarchy.get(i));
                                    // print the last artifact in the hierarchy
                                    if (i == hierarchy.size() - 1) {
                                        sb.append(" (already included here)");
                                    }
                                    sb.append(System.lineSeparator());
                                }
                                errors.computeIfAbsent(artifact.toString(), k -> new ArrayList<>())
                                        .add(sb.toString());
                            }
                            return true;
                        }

                        @Override
                        public boolean endVisit(DependencyNode node) {
                            hierarchy.pop();
                            return true;
                        }
                    });

            for (Map.Entry<String, List<String>> entry : errors.entrySet()) {
                StringBuilder sb =
                        new StringBuilder()
                                .append(entry.getKey())
                                .append(" is already included in the nar");
                if (entry.getValue().size() > 1) {
                    sb.append(" multiple times");
                }
                sb.append(":");
                for (String error : entry.getValue()) {
                    sb.append(System.lineSeparator()).append(error);
                }
                getLog().error(sb.toString());
            }

            if (!errors.isEmpty()) {
                getLog().info(
                                "Consider changing the scope from \"compile\" to \"provided\" or exclude it in case it's a transitive dependency.");
                throw new MojoFailureException("Found duplicate dependencies");
            }

        } catch (ProjectBuildingException | DependencyCollectorBuilderException e) {
            throw new MojoExecutionException("Cannot build project dependency tree", e);
        }
    }

    private String indent(int indent) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            sb.append("|  ");
        }
        sb.append("+- ");
        return sb.toString();
    }
}
