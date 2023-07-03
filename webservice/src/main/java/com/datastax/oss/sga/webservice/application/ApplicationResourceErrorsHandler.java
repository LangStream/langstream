package com.datastax.oss.sga.webservice.application;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.server.ResponseStatusException;

@ControllerAdvice
@Order(Ordered.LOWEST_PRECEDENCE)
@Slf4j
public class ApplicationResourceErrorsHandler {

    @ExceptionHandler(Throwable.class)
    ProblemDetail handleAll(Throwable exception) {
        if (exception instanceof final ResponseStatusException rs) {
            return ProblemDetail.forStatusAndDetail(rs.getStatusCode(), rs.getMessage());
        }
        log.error("Internal error", exception);
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, exception.getMessage());
    }
}
