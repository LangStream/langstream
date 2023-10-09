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

    @Override
    public String toString() {
        return "HttpRequestFailedException{"
                + "request="
                + request
                + ", response="
                + response
                + '}';
    }
}
