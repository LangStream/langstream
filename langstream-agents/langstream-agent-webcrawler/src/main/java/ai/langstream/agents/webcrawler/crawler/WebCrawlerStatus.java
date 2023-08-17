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
