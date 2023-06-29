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
package com.datastax.oss.sga.model;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ApplicationInstance {

    private Map<String, Resource> resources = new HashMap<>();
    private Map<String, Module> modules = new HashMap<>();

    private Instance instance;
    private Secrets secrets;

    public Module getModule(String module) {
        return modules.computeIfAbsent(module, m -> new Module(m));
    }
}
