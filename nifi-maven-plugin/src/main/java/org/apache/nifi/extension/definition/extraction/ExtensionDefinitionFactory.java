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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.maven.artifact.Artifact;
import org.apache.nifi.extension.definition.ExtensionDefinition;
import org.apache.nifi.extension.definition.ExtensionType;
import org.apache.nifi.extension.definition.ServiceAPIDefinition;

public class ExtensionDefinitionFactory {
    private static final String SERVICES_DIRECTORY = "META-INF/services/";

    private static final Map<ExtensionType, String> INTERFACE_NAMES = new HashMap<>();

    static {
        INTERFACE_NAMES.put(ExtensionType.PROCESSOR, "org.apache.nifi.processor.Processor");
        INTERFACE_NAMES.put(
                ExtensionType.CONTROLLER_SERVICE, "org.apache.nifi.controller.ControllerService");
        INTERFACE_NAMES.put(
                ExtensionType.REPORTING_TASK, "org.apache.nifi.reporting.ReportingTask");
    }

    private final ClassLoader extensionClassLoader;

    public ExtensionDefinitionFactory(final ClassLoader classLoader) {
        this.extensionClassLoader = classLoader;
    }

    public Set<ExtensionDefinition> discoverExtensions(final ExtensionType extensionType)
            throws IOException {
        final String interfaceName = INTERFACE_NAMES.get(extensionType);
        final Set<String> classNames = discoverClassNames(interfaceName);

        if (classNames.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<ExtensionDefinition> definitions = new HashSet<>();
        for (final String className : classNames) {
            try {
                definitions.add(createExtensionDefinition(extensionType, className));
            } catch (final Exception e) {
                throw new IOException(
                        "Failed to create Extension Definition for "
                                + extensionType
                                + " "
                                + className,
                        e);
            }
        }

        return definitions;
    }

    private ExtensionDefinition createExtensionDefinition(
            final ExtensionType extensionType, final String className)
            throws ClassNotFoundException {
        final Class<?> extensionClass = Class.forName(className, false, extensionClassLoader);
        final Set<ServiceAPIDefinition> serviceApis =
                getProvidedServiceAPIs(extensionType, extensionClass);
        return new StandardExtensionDefinition(extensionType, className, serviceApis);
    }

    private Set<ServiceAPIDefinition> getProvidedServiceAPIs(
            final ExtensionType extensionType, final Class<?> extensionClass)
            throws ClassNotFoundException {
        if (extensionType != ExtensionType.CONTROLLER_SERVICE) {
            return Collections.emptySet();
        }

        final Set<ServiceAPIDefinition> serviceApis = new HashSet<>();
        final Class<?> controllerServiceClass =
                Class.forName(
                        "org.apache.nifi.controller.ControllerService",
                        false,
                        extensionClassLoader);
        addProvidedServiceAPIs(controllerServiceClass, extensionClass, serviceApis);
        return serviceApis;
    }

    private void addProvidedServiceAPIs(
            final Class<?> controllerServiceClass,
            final Class<?> extensionClass,
            final Set<ServiceAPIDefinition> serviceApis) {
        final Class<?>[] extensionInterfaces = extensionClass.getInterfaces();
        if (extensionInterfaces == null) {
            return;
        }

        for (final Class<?> immediateInterface : extensionInterfaces) {
            final Set<Class<?>> interfaceHierarchy = new HashSet<>();
            interfaceHierarchy.add(immediateInterface);
            getInterfaceHierarchy(immediateInterface, interfaceHierarchy);

            for (final Class<?> implementedInterface : interfaceHierarchy) {
                processImplementedInterface(
                        implementedInterface, controllerServiceClass, serviceApis);
            }
        }

        if (extensionClass.getSuperclass() != null) {
            addProvidedServiceAPIs(
                    controllerServiceClass, extensionClass.getSuperclass(), serviceApis);
        }
    }

    private void getInterfaceHierarchy(
            final Class<?> implementedInterface, final Set<Class<?>> interfaceHierarchy) {
        final Class<?>[] parentInterfaces = implementedInterface.getInterfaces();
        if (parentInterfaces == null) {
            return;
        }

        for (final Class<?> parentInterface : parentInterfaces) {
            if (!interfaceHierarchy.contains(parentInterface)) {
                interfaceHierarchy.add(parentInterface);
                getInterfaceHierarchy(parentInterface, interfaceHierarchy);
            }
        }
    }

    private void processImplementedInterface(
            final Class<?> implementedInterface,
            final Class<?> controllerServiceClass,
            final Set<ServiceAPIDefinition> serviceApis) {
        if (controllerServiceClass.isAssignableFrom(implementedInterface)
                && !controllerServiceClass.equals(implementedInterface)) {
            final ClassLoader interfaceClassLoader = implementedInterface.getClassLoader();
            if (interfaceClassLoader instanceof ExtensionClassLoader) {
                final Artifact interfaceNarArtifact =
                        ((ExtensionClassLoader) interfaceClassLoader).getNarArtifact();

                final ServiceAPIDefinition serviceDefinition =
                        new StandardServiceAPIDefinition(
                                implementedInterface.getName(),
                                interfaceNarArtifact.getGroupId(),
                                interfaceNarArtifact.getArtifactId(),
                                interfaceNarArtifact.getBaseVersion());

                serviceApis.add(serviceDefinition);
            }
        }
    }

    private Set<String> discoverClassNames(final String extensionType) throws IOException {
        final Set<URL> resourceUrls = new HashSet<>();

        final Enumeration<URL> resources =
                extensionClassLoader.getResources(SERVICES_DIRECTORY + extensionType);
        while (resources.hasMoreElements()) {
            resourceUrls.add(resources.nextElement());
        }

        final ClassLoader parentClassLoader = extensionClassLoader.getParent();
        if (parentClassLoader != null) {
            final Enumeration<URL> parentResources =
                    parentClassLoader.getResources(SERVICES_DIRECTORY + extensionType);
            while (parentResources.hasMoreElements()) {
                resourceUrls.remove(parentResources.nextElement());
            }
        }

        final Set<String> classNames = new HashSet<>();
        for (final URL resourceUrl : resourceUrls) {
            classNames.addAll(discoverClassNames(resourceUrl));
        }
        return classNames;
    }

    private Set<String> discoverClassNames(final URL serviceUrl) throws IOException {
        final Set<String> classNames = new HashSet<>();

        try (final InputStream in = serviceUrl.openStream();
                final Reader rawReader = new InputStreamReader(in);
                final BufferedReader reader = new BufferedReader(rawReader)) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();

                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }

                classNames.add(line);
            }
        }

        return classNames;
    }
}
