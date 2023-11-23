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
package com.datastax.oss.streaming.ai.datasource;

import ai.langstream.api.util.ConfigurationUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.CqlVectorType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CqlVectorCodec;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.dtsx.astra.sdk.db.AstraDBOpsClient;
import com.dtsx.astra.sdk.db.DbOpsClient;
import com.dtsx.astra.sdk.utils.AstraEnvironment;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraDataSource implements QueryStepDataSource {

    CqlSession session;
    String astraToken;
    String astraEnvironment;

    String astraDatabase;
    String astraDatabaseId;
    Map<String, PreparedStatement> statements = new ConcurrentHashMap<>();

    private static final DefaultCodecRegistry CODEC_REGISTRY =
            new DefaultCodecRegistry("default-registry") {

                protected TypeCodec<?> createCodec(
                        @Nullable DataType cqlType,
                        @Nullable GenericType<?> javaType,
                        boolean isJavaCovariant) {
                    if (cqlType instanceof CqlVectorType) {
                        log.info("Automatically Registering codec for CqlVectorType {}", cqlType);
                        CqlVectorType vectorType = (CqlVectorType) cqlType;
                        return new CqlVectorCodec<>(vectorType, codecFor(vectorType.getSubtype()));
                    }
                    return super.createCodec(cqlType, javaType, isJavaCovariant);
                }
            };

    @Override
    public void initialize(Map<String, Object> dataSourceConfig) {
        log.info(
                "Initializing CassandraDataSource with config {}",
                ConfigurationUtils.redactSecrets(dataSourceConfig));
        this.astraToken = ConfigurationUtils.getString("token", "", dataSourceConfig);
        this.astraEnvironment =
                ConfigurationUtils.getString("environment", "PROD", dataSourceConfig);
        this.astraDatabase = ConfigurationUtils.getString("database", "", dataSourceConfig);
        this.astraDatabaseId = ConfigurationUtils.getString("database-id", "", dataSourceConfig);
        this.session = buildCqlSession(dataSourceConfig);
    }

    @Override
    public void close() {
        if (session != null) {
            session.close();
        }
    }

    @Override
    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Executing query {} with params {} ({})",
                    query,
                    params,
                    params.stream()
                            .map(v -> v == null ? "null" : v.getClass().toString())
                            .collect(Collectors.joining(",")));
        }
        BoundStatement bind = prepareStatement(query, params);

        List<Row> all = session.execute(bind).all();
        return all.stream()
                .map(
                        r -> {
                            Map<String, Object> result = new HashMap<>();
                            ColumnDefinitions columnDefinitions = r.getColumnDefinitions();
                            for (int i = 0; i < columnDefinitions.size(); i++) {
                                String name = columnDefinitions.get(i).getName().toString();
                                Object object = r.getObject(i);
                                if (log.isTraceEnabled()) {
                                    log.trace(
                                            "Column {} is of type {} and value {}",
                                            name,
                                            object != null ? object.getClass().toString() : "null",
                                            object);
                                }
                                result.put(name, object);
                            }
                            return result;
                        })
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, Object> executeStatement(
            String query, List<String> generatedKeys, List<Object> params) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Executing statement {} with params {} ({})",
                    query,
                    params,
                    params.stream()
                            .map(v -> v == null ? "null" : v.getClass().toString())
                            .collect(Collectors.joining(",")));
        }
        BoundStatement bind = prepareStatement(query, params);
        session.execute(bind);
        return Map.of();
    }

    private BoundStatement prepareStatement(String query, List<Object> params) {
        PreparedStatement preparedStatement =
                statements.computeIfAbsent(query, q -> session.prepare(q));

        ColumnDefinitions variableDefinitions = preparedStatement.getVariableDefinitions();
        List<Object> adaptedParameters = new ArrayList<>();
        if (variableDefinitions.size() != params.size()) {
            throw new IllegalArgumentException(
                    "Wrong number of parameters, your query needs "
                            + variableDefinitions.size()
                            + " parameters but you provided "
                            + params.size()
                            + " fields in the agent definition");
        }
        for (int i = 0; i < variableDefinitions.size(); i++) {
            Object value = params.get(i);
            ColumnDefinition columnDefinition = variableDefinitions.get(i);
            if (columnDefinition.getType() instanceof CqlVectorType && value instanceof List) {
                CqlVectorType vectorType = (CqlVectorType) columnDefinition.getType();
                if (vectorType.getSubtype() != DataTypes.FLOAT) {
                    throw new IllegalArgumentException("Only VECTOR<FLOAT,x> is supported");
                }
                CqlVector.Builder<Float> builder = CqlVector.builder();
                for (Object v : (List<Object>) value) {
                    if (v instanceof Number) {
                        builder.add(((Number) v).floatValue());
                    } else {
                        builder.add(Float.parseFloat(v + ""));
                    }
                }
                value = builder.build();
            }
            adaptedParameters.add(value);
        }

        BoundStatement bind = preparedStatement.bind(adaptedParameters.toArray(new Object[0]));
        return bind;
    }

    public void executeStatement(String query, List<Object> params) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Executing query {} with params {} ({})",
                    query,
                    params,
                    params.stream()
                            .map(v -> v == null ? "null" : v.getClass().toString())
                            .collect(Collectors.joining(",")));
        }
        BoundStatement bind = prepareStatement(query, params);

        session.execute(bind);
    }

    private CqlSession buildCqlSession(Map<String, Object> dataSourceConfig) {

        String username = ConfigurationUtils.getString("username", null, dataSourceConfig);
        String password = ConfigurationUtils.getString("password", null, dataSourceConfig);
        // these are the values used by the Astra UI
        if (username == null) {
            username = ConfigurationUtils.getString("clientId", null, dataSourceConfig);
        }
        if (password == null) {
            password = ConfigurationUtils.getString("secret", null, dataSourceConfig);
        }

        // in AstraDB you can use "token" as clientId and the AstraCS token as password
        if (username == null && astraToken != null && !astraToken.isEmpty()) {
            username = "token";
        }
        if (password == null) {
            password = astraToken;
        }

        String secureBundle = ConfigurationUtils.getString("secureBundle", "", dataSourceConfig);
        List<String> contactPoints = ConfigurationUtils.getList("contact-points", dataSourceConfig);
        String loadBalancingLocalDc =
                ConfigurationUtils.getString("loadBalancing-localDc", "", dataSourceConfig);
        int port = ConfigurationUtils.getInteger("port", 9042, dataSourceConfig);
        log.info("Username/ClientId: {}", username);
        log.info("Contact points: {}", contactPoints);
        log.info("Secure Bundle: {}", secureBundle);

        byte[] secureBundleDecoded = null;
        if (!secureBundle.isEmpty()) {
            log.info("Using the Secure Bundle ZIP provided by the configuration");
            // Remove the base64: prefix if present
            if (secureBundle.startsWith("base64:")) {
                secureBundle = secureBundle.substring("base64:".length());
            }
            secureBundleDecoded = Base64.getDecoder().decode(secureBundle);
        } else if (!astraDatabase.isEmpty() && !astraToken.isEmpty()) {
            log.info(
                    "Automatically downloading the secure bundle for database name {} from AstraDB",
                    astraDatabase);
            DbOpsClient databaseClient = this.buildAstraClient();
            secureBundleDecoded = downloadSecureBundle(databaseClient);
        } else if (!astraDatabaseId.isEmpty() && !astraToken.isEmpty()) {
            log.info(
                    "Automatically downloading the secure bundle for database id {} from AstraDB",
                    astraDatabaseId);
            DbOpsClient databaseClient = this.buildAstraClient();
            secureBundleDecoded = downloadSecureBundle(databaseClient);
        } else {
            log.info("No secure bundle provided, using the default CQL driver for Cassandra");
        }

        CqlSessionBuilder builder = new CqlSessionBuilder().withCodecRegistry(CODEC_REGISTRY);

        if (username != null && password != null) {
            builder.withAuthCredentials(username, password);
        }
        if (secureBundleDecoded != null) {
            builder.withCloudSecureConnectBundle(new ByteArrayInputStream(secureBundleDecoded));
        }
        if (!contactPoints.isEmpty()) {
            builder.addContactPoints(
                    contactPoints.stream()
                            .map(cp -> new InetSocketAddress(cp, port))
                            .collect(Collectors.toList()));
        }
        if (!loadBalancingLocalDc.isEmpty()) {
            builder.withLocalDatacenter(loadBalancingLocalDc);
        }
        return builder.build();
    }

    public static String escapeCQLString(String value) {
        return value.replace("'", "''");
    }

    public CqlSession getSession() {
        return session;
    }

    public DbOpsClient buildAstraClient() {
        return buildAstraClient(astraToken, astraDatabase, astraDatabaseId, astraEnvironment);
    }

    public static DbOpsClient buildAstraClient(
            String astraToken,
            String astraDatabase,
            String astraDatabaseId,
            String astraEnvironment) {
        if (astraToken.isEmpty()) {
            throw new IllegalArgumentException("You must configure the AstraDB token");
        }
        AstraDBOpsClient astraDbClient =
                new AstraDBOpsClient(astraToken, AstraEnvironment.valueOf(astraEnvironment));
        if (!astraDatabase.isEmpty()) {
            return astraDbClient.databaseByName(astraDatabase);
        } else if (!astraDatabaseId.isEmpty()) {
            return astraDbClient.database(astraDatabaseId);
        } else {
            throw new IllegalArgumentException(
                    "You must configure the database name or the database id");
        }
    }

    public static byte[] downloadSecureBundle(DbOpsClient databaseClient) {
        long start = System.currentTimeMillis();
        byte[] secureBundleDecoded = databaseClient.downloadDefaultSecureConnectBundle();
        long delta = System.currentTimeMillis() - start;
        log.info("Downloaded {} bytes in {} ms", secureBundleDecoded.length, delta);
        return secureBundleDecoded;
    }
}
