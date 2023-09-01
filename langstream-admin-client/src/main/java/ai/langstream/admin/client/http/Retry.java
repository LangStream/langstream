package ai.langstream.admin.client.http;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;

public interface Retry {

    Optional<Long> shouldRetryAfter(Exception exception, HttpRequest request, HttpResponse<?> response);
}
