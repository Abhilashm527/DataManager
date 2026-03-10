package com.dataflow.dataloaders.entity.dagmodels.dag;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

/**
 * DAG Edge Definition
 * Represents a connection between two nodes in the DAG
 */
@Data
@lombok.EqualsAndHashCode(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Edge extends com.dataflow.dataloaders.entity.AuditMetaData {
    private String edgeId;
    private String dataflowId;
    private String sourceNodeId;
    private String targetNodeId;
    private String sourcePort;
    private String targetPort;
    private EdgeType edgeType;

    // Edge behavior
    private EdgeCondition condition;
    private Boolean async;
    private Integer bufferSize;

    // Data transformation on edge
    private EdgeTransformation transformation;

    // Flow control
    private FlowControl flowControl;

    // Data lineage
    private Lineage lineage;

    public enum EdgeType {
        DATA_FLOW, // Normal data flow between nodes
        CONTROL_FLOW, // Execution dependency without data
        ERROR_FLOW // Error handling path
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class EdgeCondition {
        private ConditionType type;
        private String expression;

        public enum ConditionType {
            ON_SUCCESS,
            ON_FAILURE,
            ON_ERROR,
            DATA_BASED,
            ALWAYS
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class EdgeTransformation {
        private TransformationType type;
        private List<FieldMapping> mappings;
        private List<Filter> filters;

        public enum TransformationType {
            FIELD_MAPPING,
            FILTER,
            AGGREGATE,
            SPLIT,
            NESTING,
            UNNESTING
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class FieldMapping {
        private String sourcePath; // Input path (e.g., "user.billing.address")
        private String targetPath; // Output path (e.g., "transformed.billing_address")
        private MappingType mappingType;
        private String transformExpression; // SpEL, Script, or direct rename logic
        private Object defaultValue;

        public enum MappingType {
            DIRECT, // Straight passthrough
            RENAME, // Change name, keep value
            MAP_NESTED, // Move field into/out of a nested structure
            CONSTANT, // Set a fixed value
            TRANSFORM // Apply logic via transformExpression
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Filter {
        private String field;
        private FilterOperator operator;
        private Object value;

        public enum FilterOperator {
            EQUALS,
            NOT_EQUALS,
            GREATER_THAN,
            LESS_THAN,
            IN,
            NOT_IN,
            NOT_NULL,
            IS_NULL,
            CONTAINS,
            REGEX
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class FlowControl {
        private Boolean async;
        private Integer bufferSize;
        private BackpressureStrategy backpressure;
        private Integer timeout;

        public enum BackpressureStrategy {
            BLOCK,
            DROP,
            BUFFER,
            ERROR
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Lineage {
        private Boolean trackChanges;
        private List<String> auditFields;
        private String transformationDescription;
    }
}
