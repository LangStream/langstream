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
package ai.langstream.runtime.impl.k8s.agents;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class WebCrawlerSourceAgentProviderTest {
    @Test
    @SneakyThrows
    public void testStateStorage() {
        validate(
                """
                topics: []
                pipeline:
                  - name: "webcrawler-source"
                    type: "webcrawler-source"
                    configuration: {}
                """,
                null);

        validate(
                """
                topics: []
                pipeline:
                  - name: "webcrawler-source"
                    type: "webcrawler-source"
                    configuration:
                        state-storage: s3
                """,
                null);

        validate(
                """
                topics: []
                pipeline:
                  - name: "webcrawler-source"
                    type: "webcrawler-source"
                    configuration:
                        state-storage: disk
                """,
                null);
    }

    private void validate(String pipeline, String expectErrMessage) throws Exception {
        AgentValidationTestUtil.validate(pipeline, expectErrMessage);
    }
}
