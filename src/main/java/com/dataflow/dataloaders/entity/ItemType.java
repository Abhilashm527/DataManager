package com.dataflow.dataloaders.entity;


public enum ItemType {
    APPLICATIONS("Applications"),
    RESOURCES("Resources"),
    DATAFLOWS("Dataflows"),
    STORAGE ("Storage"),
    NOTIFICATION ("Notification"),
    SCHEDULER("Scheduler"),
    API_ENDPOINT ("ApiEndpoint"),
    VARIABLES ("Variables"),
    SECURITY ("Security"),
    AZURE_CONFIGS ("Azure Configurations"),
    DATATABLES ("DataTables"),
    MAPPINGS("Mappings");

    private final String value;

    ItemType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static ItemType fromString(String text) {
        for (ItemType type : ItemType.values()) {
            if (type.value.equalsIgnoreCase(text)) {
                return type;
            }
        }
        return null;
    }
}