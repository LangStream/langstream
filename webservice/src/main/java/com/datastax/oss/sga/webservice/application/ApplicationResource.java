package com.datastax.oss.sga.webservice.application;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Tag(name = "applications")
@RequestMapping("/api/applications")
public class ApplicationResource {

    @GetMapping("")
    @Operation(summary = "Get all applications")
    List<String> getApplications() {
        return List.of();
    }
}