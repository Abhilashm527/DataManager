package com.dataflow.dataloaders.entity.dag;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Detailed definition of a data field, supporting nested structures and lineage
 * tracking.
 */
@Data
public class FieldDefinition {
    private String name; // Current field name or path element (e.g., "userId" or "city")
    private DataType type; // The data type of this field
    private List<FieldDefinition> children; // Nested fields if type is OBJECT or ARRAY
    private Lineage lineage; // Visual tracking of where this field originated
    private Map<String, Object> properties; // Additional metadata (isNullable, precision, DDL type, etc.)

    public enum DataType {
        STRING, INTEGER, LONG, DOUBLE, DECIMAL, BOOLEAN, DATE, TIMESTAMP, OBJECT, ARRAY, BINARY, ANY
    }

    @Data
    public static class Lineage {
        private String sourceNodeId; // The ID of the node where this field was first created (e.g., "JDBC_READER")
        private String sourcePath; // The original name/path in the source system (e.g., "USR_ID")
        private List<String> flowTrace = new ArrayList<>(); // Sequential history of nodes/actions that touched this
                                                            // field
    }

    public void addTrace(String trace) {
        if (this.lineage == null) {
            this.lineage = new Lineage();
        }
        this.lineage.flowTrace.add(trace);
    }
}
