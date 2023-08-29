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
package com.datastax.oss.streaming.ai;

import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.model.JsonRecord;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;

/**
 * Compute AI Embeddings from a template filled with the received message fields and metadata and
 * put the value into a new or existing field.
 */
public class ComputeAIEmbeddingsStep implements TransformStep {

    private final Template template;
    private final String embeddingsFieldName;
    private final EmbeddingsService embeddingsService;
    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> avroValueSchemaCache =
            new ConcurrentHashMap<>();

    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> avroKeySchemaCache =
            new ConcurrentHashMap<>();

    public ComputeAIEmbeddingsStep(
            String text, String embeddingsFieldName, EmbeddingsService embeddingsService) {
        this.template = Mustache.compiler().compile(text);
        this.embeddingsFieldName = embeddingsFieldName;
        this.embeddingsService = embeddingsService;
    }

    @Override
    public void close() throws Exception {
        if (embeddingsService != null) {
            embeddingsService.close();
        }
    }

    @Override
    public void process(TransformContext transformContext) {
        JsonRecord jsonRecord = transformContext.toJsonRecord();
        String text = template.execute(jsonRecord);

        final List<Double> embeddings = embeddingsService.computeEmbeddings(List.of(text)).get(0);
        transformContext.setResultField(
                embeddings,
                embeddingsFieldName,
                Schema.createArray(Schema.create(Schema.Type.DOUBLE)),
                avroKeySchemaCache,
                avroValueSchemaCache);
    }
}
