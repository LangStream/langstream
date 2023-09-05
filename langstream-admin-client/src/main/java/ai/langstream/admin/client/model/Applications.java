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
package ai.langstream.admin.client.model;

import ai.langstream.admin.client.util.MultiPartBodyPublisher;
import java.net.http.HttpResponse;

public interface Applications {
    void deploy(String application, MultiPartBodyPublisher multiPartBodyPublisher);

    void update(String application, MultiPartBodyPublisher multiPartBodyPublisher);

    void delete(String application);

    String get(String application, boolean stats);

    String list();

    HttpResponse<byte[]> download(String application);

    HttpResponse<byte[]> download(String application, String codeStorageId);

    <T> HttpResponse<T> download(
            String application, HttpResponse.BodyHandler<T> responseBodyHandler);

    <T> HttpResponse<T> download(
            String application,
            String codeStorageId,
            HttpResponse.BodyHandler<T> responseBodyHandler);

    String getCodeInfo(String application, String codeArchiveId);
}
