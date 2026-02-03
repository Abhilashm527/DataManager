package com.dataflow.dataloaders.exception;


import lombok.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.validation.FieldError;

import java.util.Map;

@Component
public class ErrorFactory {
    @Generated
    private static final Logger log = LoggerFactory.getLogger(ErrorFactory.class);
    public static final String SOURCE_APPLICATION = "Dataloaders-services";
    private final Environment environment;
    public static final ErrorDefinition DUPLICATION;
    public static final ErrorDefinition MOVED_PERMANENTLY;
    public static final ErrorDefinition NOT_MODIFIED;
    public static final ErrorDefinition BAD_REQUEST;
    public static final ErrorDefinition INVALID_TIME;
    public static final ErrorDefinition UNAUTHORIZED;
    public static final ErrorDefinition MISSING_JWT_TOKEN;
    public static final ErrorDefinition INVALID_JWT_TOKEN;
    public static final ErrorDefinition GOOGLE_RECAPTCHA;
    public static final ErrorDefinition PAYMENT_REQUIRED;
    public static final ErrorDefinition FORBIDDEN;
    public static final ErrorDefinition NO_PERMISSION;
    public static final ErrorDefinition CREDENTIALS_EXPIRED;
    public static final ErrorDefinition RESOURCE_NOT_FOUND;
    public static final ErrorDefinition JSON_PROCESSING_ERROR;
    public static final ErrorDefinition TOO_LARGE;
    public static final ErrorDefinition FILE_TOO_LARGE;
    public static final ErrorDefinition UNSUPPORTED_MEDIA_TYPE;
    public static final ErrorDefinition RESOURCE_CONFLICT;
    public static final ErrorDefinition RESOURCE_LOCKED;
    public static final ErrorDefinition READ_ONLY;
    public static final ErrorDefinition TOO_MANY_REQUESTS;
    public static final ErrorDefinition RESOURCE_FAILED_VALIDATION;
    public static final ErrorDefinition EXPECTATION_FAILED;
    public static final ErrorDefinition FAILED_TO_CREATE_XML;
    public static final ErrorDefinition INTERNAL_SERVER_ERROR;
    public static final ErrorDefinition DATABASE_TIMEOUT;
    public static final ErrorDefinition DATABASE_EXCEPTION;
    public static final ErrorDefinition FAILED_TO_INSERT;
    public static final ErrorDefinition FAIL_DELETING_PERMANENTLY;
    public static final ErrorDefinition FAILED_UPDATE;
    public static final ErrorDefinition SSL_HANDSHAKE_FAILURE;
    public static final ErrorDefinition NOT_IMPLEMENTED;
    public static final ErrorDefinition SEARCH_TIMEOUT;
    public static final ErrorDefinition RESOURCE_CREATION_FAILED;
    public static final ErrorDefinition VALIDATION_ERROR;
    final Map<String, HttpStatus> httpStatus;

    @Autowired
    public ErrorFactory(Environment environment) {
        this.httpStatus = Map.ofEntries(Map.entry("204", HttpStatus.NO_CONTENT), Map.entry("304", HttpStatus.NOT_MODIFIED), Map.entry("400", HttpStatus.BAD_REQUEST), Map.entry("401", HttpStatus.FORBIDDEN), Map.entry("402", HttpStatus.PAYLOAD_TOO_LARGE), Map.entry("404", HttpStatus.NOT_FOUND), Map.entry("406", HttpStatus.NOT_ACCEPTABLE), Map.entry("409", HttpStatus.CONFLICT), Map.entry("415", HttpStatus.UNSUPPORTED_MEDIA_TYPE), Map.entry("500", HttpStatus.INTERNAL_SERVER_ERROR), Map.entry("501", HttpStatus.NOT_IMPLEMENTED), Map.entry("503", HttpStatus.SERVICE_UNAVAILABLE), Map.entry("413", HttpStatus.PAYLOAD_TOO_LARGE));
        this.environment = environment;
    }

    public static ErrorDefinition getErrorDefinition(ErrorDefinition errorDefinition, FieldError fieldError) {
        errorDefinition.setLabel(fieldError.getField());
        errorDefinition.setMessage(fieldError.getDefaultMessage());
        return errorDefinition;
    }

    public ErrorDefinition getErrorDefinitionByCode(String key) {
        Logger var10000 = log;
        String var10001 = this.getClass().getSimpleName();
        var10000.info(var10001 + ": " + Thread.currentThread().getStackTrace()[1].getMethodName());
        return this.environment.getProperty(key) == null ? this.getErrorDefinition("500001") : this.getErrorDefinition(key);
    }

    public ErrorDefinition getErrorDefinition(String key) {
        Logger var10000 = log;
        String var10001 = this.getClass().getSimpleName();
        var10000.info(var10001 + ": " + Thread.currentThread().getStackTrace()[1].getMethodName());
        String[] errorDefinitions = this.environment.getProperty(key).split("\\+");
        if (errorDefinitions != null && errorDefinitions.length == 5) {
            String label = errorDefinitions[0];
            String level = errorDefinitions[1];
            String severity = errorDefinitions[2];
            String msg = errorDefinitions[3];
            String status = errorDefinitions[4].trim();
            return new ErrorDefinition(label, key, ErrorLevel.getErrorLevel(level), ErrorSeverity.getErrorSeverity(severity), msg, (HttpStatus)this.httpStatus.get(status), SOURCE_APPLICATION);
        } else {
            log.error("There is no definition for input key: {}", key);
            throw new DataloadersException("There is no Error definition. Please contact development team");
        }
    }

    static {
        VALIDATION_ERROR = new ErrorDefinition("VALIDATION ERROR", "226001", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Validation", HttpStatus.INTERNAL_SERVER_ERROR, SOURCE_APPLICATION);
        DUPLICATION = new ErrorDefinition("DUPLICATION", "226002", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Duplication", HttpStatus.IM_USED, SOURCE_APPLICATION);
        MOVED_PERMANENTLY = new ErrorDefinition("MOVED_PERMANENTLY", "301001", ErrorLevel.REQUEST, ErrorSeverity.NONFATAL, "Moved Permanently", HttpStatus.MOVED_PERMANENTLY, SOURCE_APPLICATION);
        NOT_MODIFIED = new ErrorDefinition("NOT_MODIFIED", "304001", ErrorLevel.RESOURCE, ErrorSeverity.FATAL, "This resource is not modified.", HttpStatus.NOT_MODIFIED, SOURCE_APPLICATION);
        BAD_REQUEST = new ErrorDefinition("BAD_REQUEST", "400001", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Bad request", HttpStatus.BAD_REQUEST, SOURCE_APPLICATION);
        INVALID_TIME = new ErrorDefinition("INVALID_TIME", "400002", ErrorLevel.REQUEST, ErrorSeverity.NONFATAL, "Invalid Time Numerical Offset", HttpStatus.BAD_REQUEST, SOURCE_APPLICATION);
        UNAUTHORIZED = new ErrorDefinition("UNAUTHORIZED", "401001", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Incorrect userId & password combination or userId has been locked.", HttpStatus.UNAUTHORIZED, SOURCE_APPLICATION);
        MISSING_JWT_TOKEN = new ErrorDefinition("MISSING_JWT_TOKEN", "401002", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "The JWT token is missing.", HttpStatus.UNAUTHORIZED, SOURCE_APPLICATION);
        INVALID_JWT_TOKEN = new ErrorDefinition("INVALID_JWT_TOKEN", "401003", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "The JWT token is invalid.", HttpStatus.UNAUTHORIZED, SOURCE_APPLICATION);
        GOOGLE_RECAPTCHA = new ErrorDefinition("GOOGLE_RECAPTCHA", "401004", ErrorLevel.REQUEST, ErrorSeverity.NONFATAL, "Recaptcha not verified", HttpStatus.UNAUTHORIZED, SOURCE_APPLICATION);
        PAYMENT_REQUIRED = new ErrorDefinition("PAYMENT_REQUIRED", "402001", ErrorLevel.REQUEST, ErrorSeverity.NONFATAL, "Payment Required", HttpStatus.PAYMENT_REQUIRED, SOURCE_APPLICATION);
        FORBIDDEN = new ErrorDefinition("FORBIDDEN", "403001", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Forbidden, Requires high-level permission, contact Admin", HttpStatus.FORBIDDEN, SOURCE_APPLICATION);
        NO_PERMISSION = new ErrorDefinition("NO_PERMISSION", "403002", ErrorLevel.REQUEST, ErrorSeverity.NONFATAL, "User does not have enough permission", HttpStatus.FORBIDDEN, SOURCE_APPLICATION);
        CREDENTIALS_EXPIRED = new ErrorDefinition("CREDENTIALS_EXPIRED", "403003", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "User credentials have expired", HttpStatus.FORBIDDEN, SOURCE_APPLICATION);
        RESOURCE_NOT_FOUND = new ErrorDefinition("RESOURCE_NOT_FOUND", "404001", ErrorLevel.RESOURCE, ErrorSeverity.FATAL, "Resource not found", HttpStatus.NOT_FOUND, SOURCE_APPLICATION);
        JSON_PROCESSING_ERROR = new ErrorDefinition("JSON_PROCESSING_ERROR", "404002", ErrorLevel.RESOURCE, ErrorSeverity.FATAL, "JSON processing error", HttpStatus.NOT_FOUND, SOURCE_APPLICATION);
        TOO_LARGE = new ErrorDefinition("TOO_LARGE", "413001", ErrorLevel.REQUEST, ErrorSeverity.NONFATAL, "Size exceeds the limit", HttpStatus.PAYLOAD_TOO_LARGE, SOURCE_APPLICATION);
        FILE_TOO_LARGE = new ErrorDefinition("FILE_TOO_LARGE", "413002", ErrorLevel.REQUEST, ErrorSeverity.NONFATAL, "File size exceeds the limit", HttpStatus.PAYLOAD_TOO_LARGE, SOURCE_APPLICATION);
        UNSUPPORTED_MEDIA_TYPE = new ErrorDefinition("UNSUPPORTED_MEDIA_TYPE", "415001", ErrorLevel.REQUEST, ErrorSeverity.NONFATAL, "Unsupported media type", HttpStatus.UNSUPPORTED_MEDIA_TYPE, SOURCE_APPLICATION);
        RESOURCE_CONFLICT = new ErrorDefinition("RESOURCE_CONFLICT", "409001", ErrorLevel.CONFLICT, ErrorSeverity.NONFATAL, "Resource Conflict", HttpStatus.CONFLICT, SOURCE_APPLICATION);
        RESOURCE_LOCKED = new ErrorDefinition("RESOURCE_LOCKED", "409002", ErrorLevel.RESOURCE, ErrorSeverity.FATAL, "Resource locked", HttpStatus.LOCKED, SOURCE_APPLICATION);
        READ_ONLY = new ErrorDefinition("READ_ONLY", "409003", ErrorLevel.CONFLICT, ErrorSeverity.FATAL, "This resource is read only.", HttpStatus.FORBIDDEN, SOURCE_APPLICATION);
        TOO_MANY_REQUESTS = new ErrorDefinition("TOO_MANY_REQUESTS", "429001", ErrorLevel.REQUEST, ErrorSeverity.NONFATAL, "Too Many Requests", HttpStatus.TOO_MANY_REQUESTS, SOURCE_APPLICATION);
        RESOURCE_FAILED_VALIDATION = new ErrorDefinition("RESOURCE_FAILED_VALIDATION", "422001", ErrorLevel.BUSINESS, ErrorSeverity.FATAL, "Resource failed validation", HttpStatus.PRECONDITION_FAILED, SOURCE_APPLICATION);
        EXPECTATION_FAILED = new ErrorDefinition("EXPECTATION_FAILED", "422002", ErrorLevel.BUSINESS, ErrorSeverity.FATAL, "Expectation failed", HttpStatus.EXPECTATION_FAILED, SOURCE_APPLICATION);
        FAILED_TO_CREATE_XML = new ErrorDefinition("FAILED_TO_CREATE_XML", "422003", ErrorLevel.BUSINESS, ErrorSeverity.FATAL, "Failed to create XML", HttpStatus.EXPECTATION_FAILED, SOURCE_APPLICATION);
        INTERNAL_SERVER_ERROR = new ErrorDefinition("INTERNAL_SERVER_ERROR", "500001", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Internal server error", HttpStatus.INTERNAL_SERVER_ERROR, SOURCE_APPLICATION);
        DATABASE_TIMEOUT = new ErrorDefinition("DATABASE_TIMEOUT", "500002", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Database server timeout", HttpStatus.INTERNAL_SERVER_ERROR, SOURCE_APPLICATION);
        DATABASE_EXCEPTION = new ErrorDefinition("DATABASE_EXCEPTION", "500003", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Database Exception", HttpStatus.INTERNAL_SERVER_ERROR, SOURCE_APPLICATION);
        FAILED_TO_INSERT = new ErrorDefinition("FAILED_TO_INSERT", "500004", ErrorLevel.REQUEST, ErrorSeverity.NONFATAL, "DAO Failed to insert", HttpStatus.INTERNAL_SERVER_ERROR, SOURCE_APPLICATION);
        FAIL_DELETING_PERMANENTLY = new ErrorDefinition("FAIL_DELETING_PERMANENTLY", "500005", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Fails to delete permanently the resource", HttpStatus.INTERNAL_SERVER_ERROR, SOURCE_APPLICATION);
        FAILED_UPDATE = new ErrorDefinition("FAILED_UPDATE", "500006", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Failed to update resource", HttpStatus.INTERNAL_SERVER_ERROR, SOURCE_APPLICATION);
        SSL_HANDSHAKE_FAILURE = new ErrorDefinition("SSL_HANDSHAKE_FAILURE", "500007", ErrorLevel.SECURITY, ErrorSeverity.FATAL, "SSL Handshake Failed", HttpStatus.INTERNAL_SERVER_ERROR, SOURCE_APPLICATION);
        NOT_IMPLEMENTED = new ErrorDefinition("NOT_IMPLEMENTED", "501001", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Not implemented, contact Admin for details", HttpStatus.NOT_IMPLEMENTED, SOURCE_APPLICATION);
        SEARCH_TIMEOUT = new ErrorDefinition("SEARCH_TIMEOUT", "503001", ErrorLevel.REQUEST, ErrorSeverity.FATAL, "Search engine server timeout", HttpStatus.REQUEST_TIMEOUT, SOURCE_APPLICATION);
        RESOURCE_CREATION_FAILED = new ErrorDefinition("RESOURCE_CREATION_FAILED", "42204", ErrorLevel.BUSINESS, ErrorSeverity.FATAL, "Resource failed validation", HttpStatus.PRECONDITION_FAILED, SOURCE_APPLICATION);
    }
}

