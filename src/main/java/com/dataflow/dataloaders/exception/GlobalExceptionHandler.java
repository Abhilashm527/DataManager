package com.dataflow.dataloaders.exception;

import com.dataflow.dataloaders.util.Response;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(DataloadersException.class)
    public ResponseEntity<Response> handleCaseManagementException(DataloadersException exception) {
        Response response = new Response();
        ErrorDefinition errorDefinition = exception.getErrorDefinition();

        if (errorDefinition == null) {
            errorDefinition = ErrorFactory.INTERNAL_SERVER_ERROR;
            errorDefinition
                    .setMessage(exception.getMessage() != null ? exception.getMessage() : "Internal Server Error");
        }

        response.setCode(errorDefinition.getHttpStatus() != null ? errorDefinition.getHttpStatus().value() : 500);
        response.setMessage(errorDefinition.getMessage());

        List<ErrorDefinition> causedBy = new ArrayList<>();
        causedBy.add(errorDefinition);
        response.setCausedBy(causedBy);

        HttpStatus status = errorDefinition.getHttpStatus() != null ? errorDefinition.getHttpStatus()
                : HttpStatus.INTERNAL_SERVER_ERROR;
        return Response.errorResponse(status, response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Object> handleGlobalException(
            Exception ex, WebRequest request) {

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        String message = ex.getMessage();
        Throwable cause = ex.getCause();
        while (cause != null) {
            message += " | Caused by: " + cause.getMessage();
            cause = cause.getCause();
        }
        body.put("message", message);

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