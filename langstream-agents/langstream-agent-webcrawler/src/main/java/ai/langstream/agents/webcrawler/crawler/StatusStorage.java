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
package ai.langstream.agents.webcrawler.crawler;

import java.util.List;
import java.util.Map;

public interface StatusStorage {
    void storeStatus(Status metadata) throws Exception;

    record StoreUrlReference(String url, String type, int depth) {}

    record RobotsFile(String content, String contentType) {}

    record Status(
            List<String> remainingUrls,
            List<StoreUrlReference> urls,
            Long lastIndexEndTimestamp,
            Long lastIndexStartTimestamp,
            Map<String, RobotsFile> robotFiles,
            Map<String, String> allTimeDocuments) {}

    Status getCurrentStatus() throws Exception;
}
