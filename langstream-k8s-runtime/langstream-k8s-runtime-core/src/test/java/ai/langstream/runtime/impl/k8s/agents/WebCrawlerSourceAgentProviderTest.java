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