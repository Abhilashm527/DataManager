package com.dataflow.dataloaders.enums;

public enum ResourceType {
    ENVIRONMENT("ENVIRONMENT"),
    MOCK_API("MOCK_API"),
    COLLECTION("COLLECTION"),
    REQUEST("REQUEST"),
    VARIABLE("VARIABLE");

    private final String value;

    ResourceType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}