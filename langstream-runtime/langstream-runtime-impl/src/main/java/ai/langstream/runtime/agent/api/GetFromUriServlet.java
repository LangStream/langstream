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
package ai.langstream.runtime.agent.api;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class GetFromUriServlet extends HttpServlet {

    private final URI uri;

    public GetFromUriServlet(URI uri) {
        this.uri = uri;
    }

    public GetFromUriServlet(String uri) {
        this(URI.create(uri));
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        HttpClient client = HttpClient.newHttpClient();
        try {
            HttpResponse<byte[]> response =
                    client.send(
                            HttpRequest.newBuilder().uri(uri).build(),
                            HttpResponse.BodyHandlers.ofByteArray());
            resp.getOutputStream().write(response.body());
            response.headers()
                    .map()
                    .forEach((key, values) -> values.forEach(value -> resp.setHeader(key, value)));
            resp.setStatus(response.statusCode());
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
