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

import java.util.Objects;
import org.apache.nifi.extension.definition.ServiceAPIDefinition;

public class StandardServiceAPIDefinition implements ServiceAPIDefinition {
    private final String serviceAPIClassName;
    private final String serviceGroupId;
    private final String serviceArtifactId;
    private final String serviceVersion;

    public StandardServiceAPIDefinition(
            final String serviceAPIClassName,
            final String serviceGroupId,
            final String serviceArtifactId,
            final String serviceVersion) {
        this.serviceAPIClassName = serviceAPIClassName;
        this.serviceGroupId = serviceGroupId;
        this.serviceArtifactId = serviceArtifactId;
        this.serviceVersion = serviceVersion;
    }

    @Override
    public String getServiceAPIClassName() {
        return serviceAPIClassName;
    }

    @Override
    public String getServiceGroupId() {
        return serviceGroupId;
    }

    @Override
    public String getServiceArtifactId() {
        return serviceArtifactId;
    }

    @Override
    public String getServiceVersion() {
        return serviceVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final StandardServiceAPIDefinition that = (StandardServiceAPIDefinition) o;
        return serviceAPIClassName.equals(that.serviceAPIClassName)
                && serviceGroupId.equals(that.serviceGroupId)
                && serviceArtifactId.equals(that.serviceArtifactId)
                && serviceVersion.equals(that.serviceVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceAPIClassName, serviceGroupId, serviceArtifactId, serviceVersion);
    }
}
