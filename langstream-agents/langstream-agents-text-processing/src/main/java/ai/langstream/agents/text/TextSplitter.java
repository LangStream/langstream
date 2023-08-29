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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class TextSplitter {
    protected int chunkSize;
    protected int chunkOverlap;
    protected java.util.function.Function<String, Integer> lengthFunction;
    protected boolean keepSeparator;
    protected boolean addStartIndex;

    public TextSplitter(
            int chunkSize,
            int chunkOverlap,
            java.util.function.Function<String, Integer> lengthFunction,
            boolean keepSeparator,
            boolean addStartIndex) {
        if (chunkOverlap > chunkSize) {
            throw new IllegalArgumentException(
                    "Got a larger chunk overlap ("
                            + chunkOverlap
                            + ") than chunk size ("
                            + chunkSize
                            + "), should be smaller.");
        }
        this.chunkSize = chunkSize;
        this.chunkOverlap = chunkOverlap;
        this.lengthFunction = lengthFunction;
        this.keepSeparator = keepSeparator;
        this.addStartIndex = addStartIndex;
    }

    public abstract List<String> splitText(String text);

    public List<Document> createDocuments(List<String> texts, List<Map<String, Object>> metadatas) {
        List<Map<String, Object>> _metadatas = metadatas != null ? metadatas : new ArrayList<>();
        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            String text = texts.get(i);
            int index = -1;
            for (String chunk : splitText(text)) {
                Map<String, Object> metadata = new HashMap<>(_metadatas.get(i));
                if (addStartIndex) {
                    index = text.indexOf(chunk, index + 1);
                    metadata.put("start_index", index);
                }
                Document newDoc = new Document(chunk, metadata);
                documents.add(newDoc);
            }
        }
        return documents;
    }

    public List<Document> splitDocuments(Iterable<Document> documents) {
        List<String> texts = new ArrayList<>();
        List<Map<String, Object>> metadatas = new ArrayList<>();
        for (Document doc : documents) {
            texts.add(doc.pageContent());
            metadatas.add(doc.metadata());
        }
        return createDocuments(texts, metadatas);
    }

    protected String joinDocs(List<String> docs, String separator) {
        String text = String.join(separator, docs);
        text = text.trim();
        return text.isEmpty() ? null : text;
    }

    protected List<String> mergeSplits(List<String> splits, String separator) {
        List<String> docs = new ArrayList<>();
        List<String> currentDoc = new ArrayList<>();
        int total = 0;
        int separatorLen = lengthFunction.apply(separator);

        for (String d : splits) {
            int len = lengthFunction.apply(d);
            if (total + len + (currentDoc.size() > 0 ? separatorLen : 0) > chunkSize) {
                if (total > chunkSize) {
                    log.warn(
                            "Created a chunk of size %d, which is longer than the specified %d"
                                    .formatted(total, chunkSize));
                }
                if (!currentDoc.isEmpty()) {
                    String doc = joinDocs(currentDoc, separator);
                    if (doc != null) {
                        docs.add(doc);
                    }
                    // Keep on popping if:
                    // - we have a larger chunk than in the chunk overlap
                    // - or if we still have any chunks and the length is long
                    while (total > chunkOverlap
                            || (total + len + (currentDoc.size() > 0 ? separatorLen : 0) > chunkSize
                                    && total > 0)) {
                        total -=
                                lengthFunction.apply(currentDoc.get(0))
                                        + (currentDoc.size() > 1 ? separatorLen : 0);
                        currentDoc.remove(0);
                    }
                }
            }
            currentDoc.add(d);
            total += len + (currentDoc.size() > 1 ? separatorLen : 0);
        }

        String doc = joinDocs(currentDoc, separator);
        if (doc != null) {
            docs.add(doc);
        }

        return docs;
    }

    public static List<String> splitTextWithRegex(
            String text, String separator, boolean keepSeparator) {
        List<String> splits = new ArrayList<>();
        if (separator != null && !separator.isEmpty()) {
            if (keepSeparator) {
                Pattern pattern = Pattern.compile("(" + separator + ")");
                Matcher matcher = pattern.matcher(text);
                if (matcher.find()) {
                    if (matcher.start() != 0) {
                        splits.add(text.substring(0, matcher.start()));
                    }
                    String lastMatch = matcher.group();
                    int lastMatchEnd = matcher.end();
                    while (matcher.find()) {
                        splits.add(lastMatch + text.substring(lastMatchEnd, matcher.start()));
                        lastMatchEnd = matcher.end();
                        lastMatch = matcher.group();
                    }
                    splits.add(lastMatch + text.substring(lastMatchEnd));
                } else {
                    splits.add(text);
                }
            } else {
                splits.addAll(Arrays.asList(text.split(separator)));
            }
        } else {
            // If separator is empty, split the text into individual characters
            for (char c : text.toCharArray()) {
                splits.add(Character.toString(c));
            }
        }

        // Remove any empty splits
        splits.removeIf(String::isEmpty);
        return splits;
    }

    record Document(String pageContent, Map<String, Object> metadata) {}
}
