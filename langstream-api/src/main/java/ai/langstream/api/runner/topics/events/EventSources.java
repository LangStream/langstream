package ai.langstream.api.runner.topics.events;

import ai.langstream.api.model.Gateway;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class EventSources {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ApplicationSource {
        private String tenant;
        private String applicationId;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GatewaySource extends ApplicationSource {
        private Gateway gateway;

        @Builder
        public GatewaySource(String tenant, String applicationId, Gateway gateway) {
            super(tenant, applicationId);
            this.gateway = gateway;
        }
    }

}
