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
package ai.langstream.impl.agents.ai.steps;

import ai.langstream.api.doc.ConfigProperty;

public class BaseGenAIStepConfiguration {
    @ConfigProperty(
            description =
                    """
                    Whether this step can be composed with other steps.
                    """,
            defaultValue = "true")
    private boolean composable = true;

    @ConfigProperty(
            description =
                    """
                    Execute the step only when the condition is met.
                    You can use the expression language to reference the message.
                    Example: when: "value.first == 'f1' && value.last.toUpperCase() == 'L1'"
                    """)
    private String when;
}
