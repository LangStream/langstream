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
import com.datastax.oss.streaming.ai.model.config.DataSourceConfig;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AstraDBDataSource implements QueryStepDataSource {

    CqlSession session;
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
    public void initialize(DataSourceConfig dataSourceConfig) {
        log.info("Initializing AstraDBDataSource with config {}", dataSourceConfig);
        this.session =
                buildCqlSession(
                        dataSourceConfig.getUsername(),
                        dataSourceConfig.getPassword(),
                        dataSourceConfig.getSecureBundle());
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

    public CqlSession buildCqlSession(String username, String password, String secureBundle) {

        // Remove the base64: prefix if present
        if (secureBundle.startsWith("base64:")) {
            secureBundle = secureBundle.substring("base64:".length());
        }

        byte[] secureBundleDecoded = Base64.getDecoder().decode(secureBundle);
        CqlSessionBuilder builder =
                new CqlSessionBuilder()
                        .withCodecRegistry(CODEC_REGISTRY)
                        .withCloudSecureConnectBundle(new ByteArrayInputStream(secureBundleDecoded))
                        .withAuthCredentials(username, password);
        return builder.build();
    }
}
