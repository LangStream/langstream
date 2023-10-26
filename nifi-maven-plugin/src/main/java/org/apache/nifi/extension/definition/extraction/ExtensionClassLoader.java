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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.maven.artifact.Artifact;

public class ExtensionClassLoader extends URLClassLoader {
    private final URL[] urls;
    private final Artifact narArtifact;
    private final Collection<Artifact> allArtifacts;

    public ExtensionClassLoader(
            final URL[] urls,
            final ClassLoader parent,
            final Artifact narArtifact,
            final Collection<Artifact> otherArtifacts) {
        super(urls, parent);
        this.urls = urls;
        this.narArtifact = narArtifact;
        this.allArtifacts = new ArrayList<>(otherArtifacts);
        if (narArtifact != null) {
            allArtifacts.add(narArtifact);
        }
    }

    public ExtensionClassLoader(
            final URL[] urls,
            final Artifact narArtifact,
            final Collection<Artifact> otherArtifacts) {
        super(urls);
        this.urls = urls;
        this.narArtifact = narArtifact;
        this.allArtifacts = new ArrayList<>(otherArtifacts);
        if (narArtifact != null) {
            allArtifacts.add(narArtifact);
        }
    }

    public String getNiFiApiVersion() {
        final Collection<Artifact> artifacts = getAllArtifacts();
        for (final Artifact artifact : artifacts) {
            if (artifact.getArtifactId().equals("nifi-api")
                    && artifact.getGroupId().equals("org.apache.nifi")) {
                return artifact.getVersion();
            }
        }

        final ClassLoader parent = getParent();
        if (parent instanceof ExtensionClassLoader) {
            return ((ExtensionClassLoader) parent).getNiFiApiVersion();
        }

        return null;
    }

    public Artifact getNarArtifact() {
        return narArtifact;
    }

    public Collection<Artifact> getAllArtifacts() {
        return allArtifacts;
    }

    @Override
    public String toString() {
        return "ExtensionClassLoader["
                + narArtifact
                + ", Dependencies="
                + Arrays.asList(urls)
                + "]";
    }

    public String toTree() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ClassLoader for ")
                .append(narArtifact)
                .append(" with nifi-api version ")
                .append(getNiFiApiVersion())
                .append(":\n");

        for (final URL url : urls) {
            sb.append(url).append("\n");
        }

        final ClassLoader parent = getParent();
        if (parent instanceof ExtensionClassLoader) {
            sb.append("\n\n-------- Parent:\n");
            sb.append(((ExtensionClassLoader) parent).toTree());
        } else if (parent instanceof URLClassLoader) {
            sb.append(toTree((URLClassLoader) parent));
        }

        return sb.toString();
    }

    private String toTree(final URLClassLoader classLoader) {
        final StringBuilder sb = new StringBuilder();

        sb.append("\n\n-------- Parent:\n");
        final URL[] urls = classLoader.getURLs();

        for (final URL url : urls) {
            sb.append(url).append("\n");
        }

        final ClassLoader parent = classLoader.getParent();
        if (parent instanceof URLClassLoader) {
            final URLClassLoader urlParent = (URLClassLoader) parent;
            sb.append(toTree(urlParent));
        }

        return sb.toString();
    }
}
