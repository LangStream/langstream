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

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
/** Definition of the resources required by the agent. */
public record ResourcesSpec(Integer parallelism, Integer size, DiskSpec disk) {

    public static ResourcesSpec DEFAULT = new ResourcesSpec(1, 1, null);

    public ResourcesSpec withDefaultsFrom(ResourcesSpec higherLevel) {
        if (higherLevel == null) {
            return this;
        }
        Integer newParallelism = parallelism == null ? higherLevel.parallelism() : parallelism;
        Integer newUnits = size == null ? higherLevel.size() : size;
        DiskSpec newDisk =
                disk == null ? higherLevel.disk() : disk.withDefaultsFrom(higherLevel.disk);
        return new ResourcesSpec(newParallelism, newUnits, newDisk);
    }
}
