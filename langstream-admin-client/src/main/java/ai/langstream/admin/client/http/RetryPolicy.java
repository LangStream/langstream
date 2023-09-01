package ai.langstream.admin.client.http;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public interface RetryPolicy {

    int maxAttempts();

    long delay(int attempt);

}

