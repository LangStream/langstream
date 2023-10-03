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
package ai.langstream.webservice.doc;

import ai.langstream.api.doc.ApiConfigurationModel;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.file.Path;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DocumentationGeneratorStarter {

    @SneakyThrows
    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("Usage: <outputDirectory> <version> ");
                System.exit(-1);
            }

            final ObjectMapper jsonWriter =
                    new ObjectMapper()
                            .configure(SerializationFeature.INDENT_OUTPUT, true)
                            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

            final String outputDir = args[0];
            final String version = args[1];
            final Path agentsFile = Path.of(outputDir).resolve("api.json");
            final ApiConfigurationModel model = DocumentationGenerator.generateDocs(version);
            jsonWriter.writeValue(agentsFile.toFile(), model);
            System.out.println(
                    "Generated documentation with %d agents, %d resources, %d assets"
                            .formatted(
                                    model.agents().size(),
                                    model.resources().size(),
                                    model.assets().size()));

        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
