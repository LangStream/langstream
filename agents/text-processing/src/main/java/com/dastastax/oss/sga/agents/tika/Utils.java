package com.dastastax.oss.sga.agents.tika;

import com.datastax.oss.sga.api.runner.code.Header;

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
