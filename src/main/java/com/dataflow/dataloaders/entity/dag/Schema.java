package com.dataflow.dataloaders.entity.dag;

import lombok.Data;

import java.time.Instant;
import java.util.List;

/**
 * Represents the structure of data at any given point in the DAG.
 */
@Data
public class Schema {
    private String nodeId; // The ID of the node that produced this schema
    private String name; // Readable name for the schema
    private List<FieldDefinition> fields; // List of top-level fields
    private Instant capturedAt; // Timestamp when metadata was last inferred
    private String version; // Optional versioning for schema evolution
}
