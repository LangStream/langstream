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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

public class RecursiveCharacterTextSplitter extends TextSplitter {
    private final List<String> separators;

    public RecursiveCharacterTextSplitter(
            List<String> separators,
            boolean keepSeparator,
            int chunkSize,
            int chunkOverlap,
            Function<String, Integer> lengthFunction) {
        super(chunkSize, chunkOverlap, lengthFunction, keepSeparator, false);
        this.separators = separators != null ? separators : Arrays.asList("\n\n", "\n", " ", "");
    }

    private List<String> splitText(String text, List<String> separators) {
        List<String> finalChunks = new ArrayList<>();
        String separator = separators.get(separators.size() - 1);
        List<String> newSeparators = new ArrayList<>();
        for (int i = 0; i < separators.size(); i++) {
            String s = separators.get(i);
            if (s.isEmpty()) {
                separator = s;
                break;
            }
            if (Pattern.compile(s).matcher(text).find()) {
                separator = s;
                newSeparators = separators.subList(i + 1, separators.size());
                break;
            }
        }
        List<String> splits = splitTextWithRegex(text, separator, keepSeparator);
        List<String> goodSplits = new ArrayList<>();
        String separatorToUse = keepSeparator ? "" : separator;

        for (String s : splits) {
            if (lengthFunction.apply(s) < chunkSize) {
                goodSplits.add(s);
            } else {
                if (!goodSplits.isEmpty()) {
                    List<String> mergedText = mergeSplits(goodSplits, separatorToUse);
                    finalChunks.addAll(mergedText);
                    goodSplits.clear();
                }
                if (newSeparators.isEmpty()) {
                    finalChunks.add(s);
                } else {
                    List<String> otherInfo = splitText(s, newSeparators);
                    finalChunks.addAll(otherInfo);
                }
            }
        }

        if (!goodSplits.isEmpty()) {
            List<String> mergedText = mergeSplits(goodSplits, separatorToUse);
            finalChunks.addAll(mergedText);
        }

        return finalChunks;
    }

    public List<String> splitText(String text) {
        return splitText(text, separators);
    }
}
