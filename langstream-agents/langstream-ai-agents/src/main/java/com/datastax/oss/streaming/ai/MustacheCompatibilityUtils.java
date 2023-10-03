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

import lombok.extern.slf4j.Slf4j;

/**
 * This is a utility class to handle backward compatibility with 0.0.x applications <code>
 * {{%# value.related_documents}}
 *   {{% text}}
 * {{%/ value.related_documents}}</code> Since 0.1.x you use native Mustache, so the same template
 * looks like this: <code>{{# value.related_documents}}
 *     {{% text}}
 * {{/ value.related_documents}}</code>
 */
@Slf4j
public class MustacheCompatibilityUtils {

    /**
     * In LangStream 0.0.x in Mustache templates you had to use {{% }} to escape the template
     *
     * @param template the template to check
     * @return the template with the legacy syntax replaced
     */
    public static String handleLegacyTemplate(String template) {
        if (template == null) {
            return null;
        }
        if (template.contains("{{%")) {
            // log a warning and a stacktrace, this way we can see where the template is used
            log.warn(
                    "Legacy template syntax detected, please update your template to use the native Mustache syntax, with {{# }} and {{/ }}",
                    new Exception(
                                    "Legacy template syntax detected, please update your template to use the native Mustache syntax")
                            .fillInStackTrace());
            return template.replace("{{%", "{{");
        }
        return template;
    }
}
