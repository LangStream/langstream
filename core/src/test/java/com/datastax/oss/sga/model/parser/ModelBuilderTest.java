package com.datastax.oss.sga.model.parser;

import com.datastax.oss.sga.model.Application;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class ModelBuilderTest {

    @Test
    void testParseApplication1() throws Exception {
        Path path = Paths.get("../examples/application1");
        Application application = ModelBuilder.build(path);

        log.info("Application: {}", application);
    }
}