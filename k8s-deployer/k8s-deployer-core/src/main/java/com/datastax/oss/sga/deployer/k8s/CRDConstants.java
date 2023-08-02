/**
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
package com.datastax.oss.sga.deployer.k8s;

public class CRDConstants {

    public static final String COMMON_LABEL_APP = "app";
    public static final String APP_LABEL_APPLICATION = "sga-application";
    public static final String APP_LABEL_SCOPE = "sga-scope";
    public static final String APP_LABEL_SCOPE_DEPLOY = "deploy";
    public static final String APP_LABEL_SCOPE_DELETE = "delete";
    public static final String AGENT_LABEL_APPLICATION = "sga-application";
    public static final String AGENT_LABEL_AGENT_ID = "sga-agent";

}
