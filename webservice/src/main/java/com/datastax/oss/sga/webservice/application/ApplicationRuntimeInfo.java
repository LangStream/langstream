package com.datastax.oss.sga.webservice.application;

import com.datastax.oss.sga.api.model.StoredApplication;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class ApplicationRuntimeInfo {

    private StoredApplication storedApplication;

    private List<RunnerInfo> runners = new ArrayList<>();

    public ApplicationRuntimeInfo(StoredApplication storedApplication) {
        this.storedApplication = storedApplication;
    }

    @Data
    public static class RunnerInfo {
        private String runnerName;
        private List<PodRuntimeInfo> instances = new ArrayList<>();
    }
    @Data
    public static class PodRuntimeInfo {
        private String podName;
        private Map<String, Object> info;
    }
}
