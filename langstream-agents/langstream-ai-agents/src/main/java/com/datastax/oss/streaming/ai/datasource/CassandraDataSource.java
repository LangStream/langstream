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
import com.dtsx.astra.sdk.db.AstraDbClient;
import com.dtsx.astra.sdk.db.DatabaseClient;
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

    String astraDatabase;
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
        log.info("Initializing AstraDBDataSource with config {}", dataSourceConfig);
        this.session = buildCqlSession(dataSourceConfig);
        this.astraToken = ConfigurationUtils.getString("astra-token", null, dataSourceConfig);
        this.astraDatabase = ConfigurationUtils.getString("astra-database", null, dataSourceConfig);
    }

    @Override
    public void close() {
        if (session != null) {
            session.close();
        }
    }

    @Override
    public List<Map<String, String>> fetchData(String query, List<Object> params) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Executing query {} with params {} ({})",
                    query,
                    params,
                    params.stream()
                            .map(v -> v == null ? "null" : v.getClass().toString())
                            .collect(Collectors.joining(",")));
        }
        PreparedStatement preparedStatement =
                statements.computeIfAbsent(query, q -> session.prepare(q));

        ColumnDefinitions variableDefinitions = preparedStatement.getVariableDefinitions();
        List<Object> adaptedParameters = new ArrayList<>();
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

        List<Row> all = session.execute(bind).all();
        return all.stream()
                .map(
                        r -> {
                            Map<String, String> result = new HashMap<>();
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
                                result.put(name, object != null ? object.toString() : null);
                            }
                            return result;
                        })
                .collect(Collectors.toList());
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
        PreparedStatement preparedStatement =
                statements.computeIfAbsent(query, q -> session.prepare(q));

        ColumnDefinitions variableDefinitions = preparedStatement.getVariableDefinitions();
        List<Object> adaptedParameters = new ArrayList<>();
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

        session.execute(bind);
    }

    public static CqlSession buildCqlSession(Map<String, Object> dataSourceConfig) {

        String username = ConfigurationUtils.getString("username", null, dataSourceConfig);
        String password = ConfigurationUtils.getString("password", null, dataSourceConfig);
        String secureBundle = ConfigurationUtils.getString("secureBundle", null, dataSourceConfig);
        List<String> contactPoints = ConfigurationUtils.getList("contact-points", dataSourceConfig);
        String loadBalancingLocalDc =
                ConfigurationUtils.getString("loadBalancing-localDc", "", dataSourceConfig);
        int port = ConfigurationUtils.getInteger("port", 9042, dataSourceConfig);

        byte[] secureBundleDecoded = null;
        if (secureBundle != null && !secureBundle.isEmpty()) {
            // Remove the base64: prefix if present
            if (secureBundle.startsWith("base64:")) {
                secureBundle = secureBundle.substring("base64:".length());
            }
            secureBundleDecoded = Base64.getDecoder().decode(secureBundle);
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

    public DatabaseClient buildAstraClient() {
        if (astraToken == null || astraDatabase == null) {
            throw new IllegalArgumentException(
                    "You must configure both astra-token and astra-database");
        }
        return new AstraDbClient(astraToken).databaseByName(astraDatabase);
    }

}
