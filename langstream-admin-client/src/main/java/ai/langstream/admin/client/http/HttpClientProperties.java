package ai.langstream.admin.client.http;

import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HttpClientProperties {
    private Supplier<Retry> retry = () -> new GenericRetryExecution(new ExponentialRetryPolicy());

}
