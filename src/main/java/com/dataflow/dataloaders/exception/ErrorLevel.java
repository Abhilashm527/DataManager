package com.dataflow.dataloaders.exception;


public enum ErrorLevel {
    REQUEST,
    BUSINESS,
    SECURITY,
    CONFLICT,
    RESOURCE;

    public static ErrorLevel getErrorLevel(String level) {
        ErrorLevel var10000;
        switch (level) {
            case "REQUEST" -> var10000 = REQUEST;
            case "BUSINESS" -> var10000 = BUSINESS;
            case "SECURITY" -> var10000 = SECURITY;
            case "CONFLICT" -> var10000 = CONFLICT;
            case "RESOURCE" -> var10000 = RESOURCE;
            default -> var10000 = REQUEST;
        }

        return var10000;
    }
}

