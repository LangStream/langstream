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
