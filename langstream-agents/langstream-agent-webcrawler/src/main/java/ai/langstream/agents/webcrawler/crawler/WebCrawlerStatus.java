/**
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

import lombok.Getter;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

@Getter
public class WebCrawlerStatus {
    Deque<String> urls = new ArrayDeque<>();
    Set<String> visited = new HashSet<>();

    public boolean isVisited(String url) {
        return visited.contains(url);
    }

    public void addVisited(String url) {
        visited.add(url);
    }

    public void addUrl(String url) {
        urls.add(url);
    }

    public String nextUrl() {
        return urls.peek();
    }

    public void urlProcessed(String url) {
        urls.remove(url);
    }
}
