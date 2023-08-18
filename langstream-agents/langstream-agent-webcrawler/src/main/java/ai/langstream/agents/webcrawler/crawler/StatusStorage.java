package ai.langstream.agents.webcrawler.crawler;

import java.util.Map;

public interface StatusStorage {
    void storeStatus(Map<String, Object> metadata) throws Exception;

    Map<String, Object> getCurrentStatus() throws Exception;
}
