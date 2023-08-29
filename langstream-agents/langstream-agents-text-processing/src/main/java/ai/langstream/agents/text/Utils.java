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

import ai.langstream.api.runner.code.Header;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Utils {
    public static InputStream toStream(Object value) {
        final InputStream stream;
        if (value instanceof byte[] array) {
            stream = new ByteArrayInputStream(array);
        } else {
            stream = new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_8));
        }
        return stream;
    }

    public static Reader toReader(Object value) {
        if (value == null) {
            return new StringReader("");
        }
        if (value instanceof byte[] array) {
            return new InputStreamReader(new ByteArrayInputStream(array), StandardCharsets.UTF_8);
        } else {
            return new StringReader(value.toString());
        }
    }

    public static String toText(Object value) {

        if (value == null) {
            return null;
        }
        if (value instanceof byte[] array) {
            return new String(array, StandardCharsets.UTF_8);
        } else {
            return value.toString();
        }
    }

    public static List<Header> addHeader(Collection<Header> headers, Header newHeader) {
        if (headers == null || headers.isEmpty()) {
            return List.of(newHeader);
        }
        List<Header> result = new ArrayList<>(headers);
        result.add(newHeader);
        return result;
    }
}
