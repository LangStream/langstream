package ai.langstream.ai.agents.services.impl.bedrock;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

public abstract class BaseInvokeModelRequest<S> {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    @SneakyThrows
    public String generateJsonBody() {
        return MAPPER.writeValueAsString(getBodyObject());
    }

    public abstract S getBodyObject();

    public abstract String getModelId();

}
