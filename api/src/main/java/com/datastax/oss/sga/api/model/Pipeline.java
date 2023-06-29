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
package com.datastax.oss.sga.api.model;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class Pipeline {
    private final String id;
    private final String module;
    private String name;

    public Pipeline(String id, String module) {
        this.id = id;
        this.module = module;
    }

    private Map<String, AgentConfiguration> agents = new HashMap<>();

    public void addAgentConfiguration(AgentConfiguration a) {
        agents.put(a.getId(), a);
    }
}
