package com.dataflow.dataloaders.exception;

import lombok.Generated;

import java.security.Timestamp;

public class DataloadersException extends RuntimeException {
    private static final long serialVersionUID = -6038483928917518709L;
    private ErrorDefinition errorDefinition;
    private Timestamp timestamp;

    public DataloadersException(ErrorDefinition errorDefinition) {
        this.errorDefinition = errorDefinition;
    }

    public DataloadersException(String message) {
        super(message);
    }

    public DataloadersException(Throwable cause) {
        super(cause);
    }

    public DataloadersException(ErrorDefinition errorDefinition, String message) {
        errorDefinition.setMessage(message);
        this.errorDefinition = errorDefinition;
    }

    public DataloadersException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataloadersException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    @Generated
    public ErrorDefinition getErrorDefinition() {
        return this.errorDefinition;
    }

    @Generated
    public Timestamp getTimestamp() {
        return this.timestamp;
    }

    @Generated
    public DataloadersException() {
    }

    @Generated
    public DataloadersException(final ErrorDefinition errorDefinition, final Timestamp timestamp) {
        this.errorDefinition = errorDefinition;
        this.timestamp = timestamp;
    }
}
