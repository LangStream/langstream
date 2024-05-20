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
import java.io.InputStream;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

public interface Applications {
    String deploy(String application, MultiPartBodyPublisher multiPartBodyPublisher);

    String deploy(
            String application,
            MultiPartBodyPublisher multiPartBodyPublisher,
            boolean dryRun,
            boolean autoUpgrade);

    void update(
            String application,
            MultiPartBodyPublisher multiPartBodyPublisher,
            boolean autoUpgrade,
            boolean forceRestart);

    void delete(String application, boolean force);

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

    HttpResponse<InputStream> logs(String application, List<String> filter, String format);

    String deployFromArchetype(
            String name, String archetypeId, Map<String, Object> parameters, boolean dryRun);
}
