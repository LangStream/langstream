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
package com.datastax.oss.streaming.ai.embeddings;

import ai.djl.MalformedModelException;
import ai.djl.repository.zoo.ModelNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * EmbeddingsService implementation using HuggingFace models adapted for use in the DJL. Thread safe
 * (but uses predictor per thread).
 *
 * <p>The model requested there should be trained for "sentence similarity" task. See
 * https://github.com/deepjavalibrary/djl/blob/master/extensions/tokenizers/README.md for model
 * conversion steps. E.g.:
 *
 * <p>python3 -m pip install -r ./extensions/tokenizers/src/main/python/requirements.txt python3
 * ./extensions/tokenizers/src/main/python/model_zoo_importer.py -m kmariunas/bert-uncased-triplet50
 * find . | grep /bert-uncased-triplet50.zip
 */
public class HuggingFaceEmbeddingService
        extends AbstractHuggingFaceEmbeddingService<String, float[]> {
    public HuggingFaceEmbeddingService(HuggingFaceConfig conf)
            throws IOException,
                    ModelNotFoundException,
                    MalformedModelException,
                    IllegalAccessException,
                    InterruptedException {
        super(conf);
    }

    @Override
    List<String> convertInput(List<String> texts) {
        return texts;
    }

    @Override
    List<List<Double>> convertOutput(List<float[]> result) {
        List<List<Double>> out = new ArrayList<>(result.size());
        for (float[] floats : result) {
            List<Double> l = new ArrayList<>(floats.length);
            for (float aFloat : floats) {
                l.add((double) aFloat);
            }
            out.add(l);
        }
        return out;
    }
}
