package ai.langstream.admin.client;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import lombok.Getter;

@Getter
public class HttpRequestFailedException extends Exception {
    private final HttpRequest request;
    private final HttpResponse<?> response;

    public HttpRequestFailedException(HttpRequest request, HttpResponse<?> response) {
        this.request = request;
        this.response = response;
    }
}
