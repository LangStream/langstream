package com.datastax.oss.sga.model.parser;

import com.datastax.oss.sga.model.ApplicationInstance;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

@Slf4j
class ModelBuilderTest {

    @Test
    void testParseApplication1() throws Exception {
        Path path = Paths.get("../examples/application1");
        ApplicationInstance application = ModelBuilder.buildApplicationInstance(Arrays.asList(path));

        log.info("Application: {}", application);
    }
}