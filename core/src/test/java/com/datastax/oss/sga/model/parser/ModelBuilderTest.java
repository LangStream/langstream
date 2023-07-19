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
package com.datastax.oss.sga.model.parser;

import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

@Slf4j
class ModelBuilderTest {

    @Test
    void testParseApplication1() throws Exception {
        Path path = Paths.get("src/test/resources/application1");
        ModelBuilder.buildApplicationInstance(Arrays.asList(path));
    }
}