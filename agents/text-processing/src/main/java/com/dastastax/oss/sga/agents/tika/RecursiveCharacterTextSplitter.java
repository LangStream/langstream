package com.dastastax.oss.sga.agents.tika;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

public class RecursiveCharacterTextSplitter extends TextSplitter {
    private List<String> separators;

    public RecursiveCharacterTextSplitter(List<String> separators, boolean keepSeparator,
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
        String separatorToUse = keepSeparator ? separator : "";

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
