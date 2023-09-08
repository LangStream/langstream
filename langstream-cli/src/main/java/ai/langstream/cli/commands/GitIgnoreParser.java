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
package ai.langstream.cli.commands;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GitIgnoreParser {
    private final Path path;
    private List<IgnoreRule> rules;

    public GitIgnoreParser(Path path) {
        this.path = path;
    }

    public boolean matches(String filePath, boolean isDir) throws IOException {
        return matches(Path.of(filePath).toAbsolutePath(), isDir);
    }

    public boolean matches(File file) throws IOException {
        return matches(file.toPath(), file.isDirectory());
    }

    public boolean matches(Path filePath, boolean isDir) throws IOException {
        if (rules == null) {
            rules = parseGitIgnore();
        }

        boolean matches = false;
        for (IgnoreRule rule : rules) {
            if (rule.match(filePath, isDir)) {
                matches = !rule.negation();
            }
        }
        return matches;
    }

    public List<IgnoreRule> parseGitIgnore() throws IOException {
        List<IgnoreRule> rules = new ArrayList<>();
        try (Scanner scanner = new Scanner(path.toFile())) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                rules.addAll(rulesFromPattern(line));
            }
        }
        return rules;
    }

    /**
     * Take a .gitignore match pattern, such as {@code *.py[cod]}, and return a list of IgnoreRule
     * suitable for matching against files and directories. Patterns which do not match files, such
     * as comments and blank lines, will return an empty list.
     *
     * @param pattern the pattern
     * @return the list of IgnoreRule
     */
    private List<IgnoreRule> rulesFromPattern(String pattern) {
        List<IgnoreRule> rules = new ArrayList<>();
        // Early returns follow
        // Discard comments and separators
        if (pattern.trim().isEmpty() || pattern.startsWith("#")) {
            return rules;
        }
        // Discard anything with more than two consecutive asterisks
        if (pattern.contains("***")) {
            return rules;
        }
        // Strip leading bang before examining double asterisks
        boolean negation = false;
        if (pattern.startsWith("!")) {
            negation = true;
            pattern = pattern.substring(1);
        }
        // Discard anything with invalid double-asterisks -- they can appear
        // at the start or the end, or be surrounded by slashes
        for (Matcher m = Pattern.compile("\\*\\*").matcher(pattern); m.find(); ) {
            int start_index = m.start();
            if (start_index != 0
                    && start_index != pattern.length() - 2
                    && (pattern.charAt(start_index - 1) != '/'
                            || pattern.charAt(start_index + 2) != '/')) {
                return rules;
            }
        }

        // Special-casing '/', which doesn't match any files or directories
        if (pattern.trim().equals("/")) {
            return rules;
        }

        // Strip leading double asterisks
        if (pattern.startsWith("**/")) {
            pattern = pattern.substring(3);
        }

        // A slash is a sign that we're tied to the base_path of our rule set.
        boolean anchored = pattern.substring(0, pattern.length() - 1).contains("/");
        if (anchored) {
            // Strip leading slashes
            while (pattern.startsWith("/")) {
                pattern = pattern.substring(1);
            }
        }

        // Trailing slash means we only match directories
        boolean directoryOnly = false;
        if (pattern.endsWith("/")) {
            directoryOnly = true;
            pattern = pattern.substring(0, pattern.length() - 1);
        }
        // patterns with leading hashes are escaped with a backslash in front, unescape it
        if (pattern.startsWith("\\#")) {
            pattern = pattern.substring(1);
        }
        // trailing spaces are ignored unless they are escaped with a backslash
        int i = pattern.length() - 1;
        boolean stripTrailingSpaces = true;
        while (i > 1 && pattern.charAt(i) == ' ') {
            if (pattern.charAt(i - 1) == '\\') {
                pattern = pattern.substring(0, i - 1) + pattern.substring(i);
                i--;
                stripTrailingSpaces = false;
            } else {
                if (stripTrailingSpaces) {
                    pattern = pattern.substring(0, i);
                }
            }
            i--;
        }

        PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
        rules.add(
                new IgnoreRule(
                        pathMatcher, negation, directoryOnly, path.toAbsolutePath().getParent()));

        if (!anchored) {
            // If we're not anchored, we need to add a second rule, so that we match files anywhere
            // in the tree.
            PathMatcher anywherePathMatcher =
                    FileSystems.getDefault().getPathMatcher("glob:**/" + pattern);
            rules.add(
                    new IgnoreRule(
                            anywherePathMatcher,
                            negation,
                            directoryOnly,
                            path.toAbsolutePath().getParent()));
        }
        return rules;
    }

    private static final class IgnoreRule {
        private final PathMatcher pathMatcher;
        private final boolean negation;
        private final boolean directoryOnly;
        private final Path basePath;

        private IgnoreRule(
                PathMatcher pathMatcher, boolean negation, boolean directoryOnly, Path basePath) {
            this.pathMatcher = pathMatcher;
            this.negation = negation;
            this.directoryOnly = directoryOnly;
            this.basePath = basePath;
        }

        public boolean match(Path path, boolean isDir) {
            if (directoryOnly && !isDir) {
                return false;
            }
            Path relPath = basePath.relativize(path.toAbsolutePath());
            return pathMatcher.matches(relPath);
        }

        public boolean negation() {
            return negation;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            var that = (IgnoreRule) obj;
            return Objects.equals(this.pathMatcher, that.pathMatcher)
                    && this.negation == that.negation
                    && this.directoryOnly == that.directoryOnly
                    && Objects.equals(this.basePath, that.basePath);
        }
    }
}
