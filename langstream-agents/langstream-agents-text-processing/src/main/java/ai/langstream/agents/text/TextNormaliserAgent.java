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
import java.util.Locale;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TextNormaliserAgent extends SingleRecordAgentProcessor {

    private boolean makeLowercase = true;
    private boolean trimSpaces = true;

    @Override
    public void init(Map<String, Object> configuration) {
        makeLowercase =
                Boolean.parseBoolean(
                        configuration.getOrDefault("make-lowercase", "true").toString());
        trimSpaces =
                Boolean.parseBoolean(configuration.getOrDefault("trim-spaces", "true").toString());
    }

    @Override
    public List<Record> processRecord(Record record) {
        if (record == null) {
            return List.of();
        }
        Object value = record.value();
        String stream = Utils.toText(value);

        if (trimSpaces) {
            stream = trimSpaces(stream);
        }
        if (makeLowercase) {
            stream = stream.toLowerCase(Locale.ENGLISH);
        }
        return List.of(SimpleRecord.copyFrom(record).value(stream).build());
    }

    static String trimSpaces(String stream) {
        return stream.replaceAll("\t+", " ") // convert tabs to spaces
                .replaceAll(" +", " ") // remove multiple spaces
                .replaceAll("\n\n\n", "\n\n") // remove repeated newlines (3 or more)
                .replaceAll(
                        "( \n\n)+", " \n\n") // remove repeated sequences of space + double newlines
                .replaceAll("( \n)+", "\n") // remove repeated sequences of space + newlines
                .replaceAll("\n ", "\n") // remove repeated sequences of newlines + space
                .trim();
    }
}
