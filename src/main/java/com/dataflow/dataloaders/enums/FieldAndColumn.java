package com.dataflow.dataloaders.enums;
import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Getter;

@Getter
public enum FieldAndColumn {
    connection_name("connection_name"),
    connection_applicationId("application_id"),
    connection_providerId("provider_id"),
    connection_isActive("is_active"),
    connection_lastTestStatus("last_test_status"),
    connection_isFavorite("is_favorite");



    private final String value;

    FieldAndColumn(String value) {
        this.value = value;
    }

    @JsonCreator
    public static FieldAndColumn fromValue(String value) {
        for (FieldAndColumn type : FieldAndColumn.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown field: " + value);
    }
}
