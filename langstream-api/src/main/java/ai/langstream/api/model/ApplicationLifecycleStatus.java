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
package ai.langstream.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApplicationLifecycleStatus {

    public static final ApplicationLifecycleStatus CREATED =
            new ApplicationLifecycleStatus(Status.CREATED, null);
    public static final ApplicationLifecycleStatus DEPLOYING =
            new ApplicationLifecycleStatus(Status.DEPLOYING, null);
    public static final ApplicationLifecycleStatus DEPLOYED =
            new ApplicationLifecycleStatus(Status.DEPLOYED, null);
    public static final ApplicationLifecycleStatus DELETING =
            new ApplicationLifecycleStatus(Status.DELETING, null);

    public static final ApplicationLifecycleStatus errorDeploying(String reason) {
        return new ApplicationLifecycleStatus(Status.ERROR_DEPLOYING, reason);
    }

    public static final ApplicationLifecycleStatus errorDeleting(String reason) {
        return new ApplicationLifecycleStatus(Status.ERROR_DELETING, reason);
    }

    private Status status;
    private String reason;

    public enum Status {
        CREATED,
        DEPLOYING,
        DEPLOYED,
        ERROR_DEPLOYING,
        DELETING,
        ERROR_DELETING
    }
}
