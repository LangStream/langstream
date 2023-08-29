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
package ai.langstream.api.runner.code;

import java.util.Collection;

public interface Record {
    Object key();

    Object value();

    String origin();

    Long timestamp();

    Collection<Header> headers();

    default Header getHeader(String key) {
        return headers().stream().filter(h -> h.key().equals(key)).findFirst().orElse(null);
    }
}
