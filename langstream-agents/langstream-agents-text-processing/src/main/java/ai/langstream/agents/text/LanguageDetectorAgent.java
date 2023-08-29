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
package ai.langstream.agents.text;

import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.langdetect.tika.LanguageIdentifier;

@Slf4j
public class LanguageDetectorAgent extends SingleRecordAgentProcessor {

    private String property = "language";
    private List<String> allowedLanguages;

    @Override
    public void init(Map<String, Object> configuration) {
        if (configuration.containsKey("property")) {
            property = (String) configuration.get("property");
        }
        if (configuration.containsKey("allowedLanguages")) {
            allowedLanguages = (List<String>) configuration.get("allowedLanguages");
        } else {
            allowedLanguages = List.of();
        }
        log.info(
                "Configuring Language Detectors with field {} and allowed languages {}",
                property,
                allowedLanguages);
    }

    @Override
    public List<Record> processRecord(Record record) {
        if (record == null) {
            return List.of();
        }
        String inputText = Utils.toText(record.value());

        LanguageIdentifier identifier = new LanguageIdentifier(inputText);
        String language = identifier.getLanguage();

        if (!allowedLanguages.isEmpty() && !allowedLanguages.contains(language)) {
            log.info(
                    "Skipping record with language {} not in allowed languages {}",
                    language,
                    allowedLanguages);
            return List.of();
        }

        Record result =
                SimpleRecord.copyFrom(record)
                        .headers(
                                Utils.addHeader(
                                        record.headers(),
                                        SimpleRecord.SimpleHeader.of(property, language)))
                        .build();

        return List.of(result);
    }
}
