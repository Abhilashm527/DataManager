package com.dataflow.dataloaders.exception;

import com.dataflow.dataloaders.util.Response;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(DataloadersException.class)
    public ResponseEntity<Response> handleCaseManagementException(DataloadersException exception) {
        Response response = new Response();
        response.setCode(exception.getErrorDefinition().getHttpStatus().value());
        response.setMessage(exception.getErrorDefinition().getMessage());

        List<ErrorDefinition> causedBy = new ArrayList<>();
        causedBy.add(exception.getErrorDefinition());
        response.setCausedBy(causedBy);
        return Response.errorResponse(exception.getErrorDefinition().getHttpStatus(), response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Object> handleGlobalException(
            Exception ex, WebRequest request) {

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", LocalDateTime.now());
        body.put("message", ex.getMessage());

        return new ResponseEntity<>(body, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<Object> handleNoResourceFoundException(
            NoResourceFoundException ex, WebRequest request) {

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", LocalDateTime.now());
        body.put("message", "Requested resource not found");

        return new ResponseEntity<>(body, HttpStatus.NOT_FOUND);
    }
}