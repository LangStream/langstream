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

import java.util.regex.Pattern;

public class CRDConstants {

    public static final String COMMON_LABEL_APP = "app";
    public static final String APP_LABEL_APPLICATION = "sga-application";
    public static final String APP_LABEL_SCOPE = "sga-scope";
    public static final String APP_LABEL_SCOPE_DEPLOY = "deploy";
    public static final String APP_LABEL_SCOPE_DELETE = "delete";
    public static final String AGENT_LABEL_APPLICATION = "sga-application";
    public static final String AGENT_LABEL_AGENT_ID = "sga-agent";


    public static final String JOB_PREFIX_CLEANUP = "sga-runtime-deployer-cleanup-";
    public static final String JOB_PREFIX_DEPLOYER = "sga-runtime-deployer-";

    // 63 is the max length for a pod.
    // - 30 for the prefix
    // - 11 for the pod template hash + separator
    // - 20 for the sum of application-id and agent-id
    // - 1+1 for a minimal 1-char agent
    public static final int MAX_APPLICATION_ID_LENGTH = 20;

    // 63 is the max length for a pod.
    // - 20 for the application id
    // - 1 for the separator
    // - 5 for the statefulset replicas index (up to replica n. 9999)
    public static final int MAX_AGENT_ID_LENGTH = 37;

    public static final Pattern RESOURCE_NAME_PATTERN = Pattern.compile("^([a-z])[-_a-z0-9]+$");

}
