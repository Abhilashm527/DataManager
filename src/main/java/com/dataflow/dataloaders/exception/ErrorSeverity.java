package com.dataflow.dataloaders.exception;

public enum ErrorSeverity {
    FATAL,
    NONFATAL;

    public static ErrorSeverity getErrorSeverity(String severity) {
        if (severity.equals("FATAL")) {
            return FATAL;
        } else {
            return severity.equals("NONFATAL") ? NONFATAL : NONFATAL;
        }
    }
}
