package com.dataflow.dataloaders.entity.dagmodels.dag;

import com.dataflow.dataloaders.entity.JobConfig;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import org.springframework.data.annotation.Id;
import java.util.List;
import java.util.Map;

/**
 * DAG Node Definition
 * Represents a single processing unit in the DAG
 */
@Data
@lombok.EqualsAndHashCode(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Node extends com.dataflow.dataloaders.entity.AuditMetaData {
    @Id
    private String nodeId;
    private String dataflowId;
    private String nodeName;
    private NodeType nodeType;
    private String componentRef;
    private String description;

    private Position position;

    // Direct Schema access for this node
    private Schema nodeSchema;

    // Node configuration
    private JobConfig config;

    // Ports for data flow
    private List<Port> inputPorts;
    private List<Port> outputPorts;

    // Execution metadata
    private String stage;
    private Integer stageOrder;
    private List<String> dependsOn;
    private ExecutionMode executionMode;

    // Resource allocation
    private Resources resources;

    // Error handling
    private NodeErrorHandling errorHandling;

    // Retry policy
    private RetryPolicy retryPolicy;

    // Validations
    private List<Validation> validations;

    // Checkpoint support
    private Checkpoint checkpoint;

    // Field Mapper configuration
    private FieldMapperConfig mapperConfig;

    // Transformation metadata
    private TransformationMetadata transformationMetadata;

    public enum NodeType {
        // Readers
        JDBC_READER,
        API_READER,
        FILE_READER,
        KAFKA_READER,
        MONGODB_READER,
        DATATABLE_READER,

        // Processors
        ENRICHER,
        VALIDATOR,
        TRANSFORMER,
        FILTER,
        AGGREGATOR,
        SPLITTER,
        MAPPER,

        // Writers
        JDBC_WRITER,
        FILE_WRITER,
        KAFKA_WRITER,
        API_WRITER,
        MONGODB_WRITER,
        ERROR_REPLAY_READER,
        DATATABLE_WRITER,

        // Control nodes
        DECISION,
        PARALLEL_SPLIT,
        JOIN,
        TRIGGER,
        SUBFLOW
    }

    public enum ExecutionMode {
        SEQUENTIAL,
        WAIT_ALL,
        WAIT_ANY
    }

    @Data
    public static class Position {
        private Integer x;
        private Integer y;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Port {
        private String portId;
        private String portName;
        private String dataType;
        private Boolean required;
        private Integer bufferSize;
        private Boolean fanOut;
        private List<String> sourceNodes;
        private Schema schema; // Formal schema for this port
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Resources {
        private String cpu;
        private String memory;
        private Integer timeout;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class NodeErrorHandling {
        private ErrorStrategy strategy;
        private Integer maxRetries;
        private Integer retryDelay;
        private Integer maxErrors;
        private Double errorThreshold;
        private String errorOutputPort;
        private String onThresholdExceeded;

        public enum ErrorStrategy {
            RETRY,
            SKIP_AND_LOG,
            FAIL,
            ROUTE_TO_DLQ,
            RETRY_THEN_SKIP,
            RETRY_THEN_FAIL
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class RetryPolicy {
        private Integer maxAttempts;
        private Integer backoffDelay;
        private Double backoffMultiplier;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Validation {
        private ValidationType type;
        private List<String> fields;
        private String expression;

        public enum ValidationType {
            NOT_NULL,
            CUSTOM,
            RANGE,
            REGEX,
            EMAIL_FORMAT
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Checkpoint {
        private Boolean enabled;
        private CheckpointStrategy strategy;
        private String checkpointField;
        private Integer checkpointInterval;
        private Boolean restartable;

        public enum CheckpointStrategy {
            OFFSET_BASED,
            TIMESTAMP_BASED,
            CUSTOM
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class TransformationMetadata {
        private List<String> inputFields;
        private List<String> outputFields;
        private String transformationType;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class FieldMapperConfig {
        private List<MappingEntry> mappings;

        @Data
        public static class MappingEntry {
            private String sourcePath; // e.g., "first_name" or "id"
            private String targetPath; // e.g., "full_name" or "user_id"
            private List<String> transforms; // e.g., ["CONCAT", "TRIM", "TO_UPPER"]
            private String expression; // Optional SpEL expression if source is complex
            private Map<String, Object> metadata; // Extra UI info (colors, icons)
        }
    }
}
