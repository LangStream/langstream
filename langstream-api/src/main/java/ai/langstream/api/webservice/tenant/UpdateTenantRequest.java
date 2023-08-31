package ai.langstream.api.webservice.tenant;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UpdateTenantRequest {

    private Integer maxTotalResourceUnits;
}
