package ai.langstream.impl.resources.datasource;

import static ai.langstream.api.util.ConfigurationUtils.validateInteger;
import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ResourceConfig;
import ai.langstream.api.model.Resource;
import ai.langstream.impl.resources.BaseDataSourceResourceProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import java.util.Map;
import lombok.Data;

@Data
@ResourceConfig(name = "JDBC", description = "Connect to any JDBC compatible database. The driver must be provided as dependency")
public class JDBCDatasourceConfig {


    public static final BaseDataSourceResourceProvider.DatasourceConfig CONFIG =
            new BaseDataSourceResourceProvider.DatasourceConfig() {
                @Override
                public Class getResourceConfigModelClass() {
                    return JDBCDatasourceConfig.class;
                }
                @Override
                public void validate(Resource resource) {
                    ClassConfigValidator.validateResourceModelFromClass(resource,
                            JDBCDatasourceConfig.class, resource.configuration(), true);
                }
            };

    @ConfigProperty(
            description =
                    """
                            JDBC entry-point driver class.
                            """,
            required = true
    )
    private String driverClass;
    @ConfigProperty(
            description =
                    """
                            JDBC connection url.
                            """,
            required = true
    )
    private String url;
}
