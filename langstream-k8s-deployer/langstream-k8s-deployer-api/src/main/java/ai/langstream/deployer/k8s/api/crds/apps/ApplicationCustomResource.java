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
package ai.langstream.deployer.k8s.api.crds.apps;

import ai.langstream.api.model.ApplicationLifecycleStatus;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1alpha1")
@Group("langstream.ai")
@Kind("Application")
@Singular("application")
@Plural("applications")
@ShortNames({"app"})
public class ApplicationCustomResource extends CustomResource<ApplicationSpec, ApplicationStatus> implements Namespaced {

    @Override
    protected ApplicationStatus initStatus() {
        final ApplicationStatus status = new ApplicationStatus();
        status.setStatus(ApplicationLifecycleStatus.CREATED);
        return status;
    }

}
