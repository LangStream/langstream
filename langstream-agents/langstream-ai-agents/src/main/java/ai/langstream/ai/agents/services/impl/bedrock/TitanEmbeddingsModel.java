package ai.langstream.ai.agents.services.impl.bedrock;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class TitanEmbeddingsModel extends BaseInvokeModelRequest<TitanEmbeddingsModel.RequestBody> {

    private String modelId;
    private String inputText;

    public record RequestBody(String inputText) {
    }
    public record ResponseBody(List<Double> embedding) {
    }

    @Override
    public String getModelId() {
        return modelId;
    }

    @Override
    public RequestBody getBodyObject() {
        return new RequestBody(inputText);
    }
}
