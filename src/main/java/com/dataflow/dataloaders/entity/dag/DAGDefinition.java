package com.dataflow.dataloaders.entity.dag;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * DAG (Directed Acyclic Graph) Definition
 * Represents a complete data processing pipeline as a DAG
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DAGDefinition {
    private String dagId;
    private String dagName;
    private String description;
    private DAGType type;
    private String version;
    private DAGStatus status;
    private String createdBy;
    private Instant createdAt;
    private Instant lastModified;
    private List<String> tags;

    // Global configuration
    private GlobalProperties globalProperties;
    private Schedule schedule;
    private ExecutionPlan executionPlan;

    // DAG structure
    private List<Node> nodes;
    private List<Edge> edges;

    // Operational configuration
    private ErrorHandling errorHandling;
    private Monitoring monitoring;
    private ResourceManagement resourceManagement;
    private DAGMetadata metadata;

    public enum DAGType {
        BATCH,
        STREAMING,
        HYBRID
    }

    public enum DAGStatus {
        DRAFT,
        ACTIVE,
        INACTIVE,
        DEPRECATED,
        ARCHIVED
    }

    @Data
    public static class GlobalProperties {
        private String environment;
        private Integer maxConcurrency;
        private Integer defaultTimeout;
        private String notificationEmail;
        private Map<String, Object> customProperties;
    }

    @Data
    public static class Schedule {
        private ScheduleType type;
        private String cronExpression;
        private String timezone;
        private Boolean enabled;
        private List<String> triggerEvents;
        private List<String> dependsOnTopologies;
        private Boolean retryOnFailure;
        private Integer maxRetries;

        public enum ScheduleType {
            CRON,
            INTERVAL,
            EVENT_DRIVEN,
            MANUAL
        }
    }

    @Data
    public static class ExecutionPlan {
        private ExecutionType type;
        private ParallelismStrategy parallelismStrategy;
        private Integer maxParallelNodes;
        private List<Stage> stages;

        public enum ExecutionType {
            DAG,
            LINEAR,
            HYBRID
        }

        public enum ParallelismStrategy {
            AUTO,
            MANUAL,
            RESOURCE_BASED
        }
    }

    @Data
    public static class Stage {
        private String stageId;
        private String stageName;
        private List<String> nodes;
        private ExecutionMode executionMode;
        private List<String> dependsOn;

        public enum ExecutionMode {
            SEQUENTIAL,
            PARALLEL,
            CONDITIONAL
        }
    }

    @Data
    public static class ErrorHandling {
        private ErrorStrategy defaultStrategy;
        private Integer maxRetries;
        private Integer retryDelay;
        private OnFailure onFailure;
        private DeadLetterQueue deadLetterQueue;

        public enum ErrorStrategy {
            RETRY_THEN_FAIL,
            RETRY_THEN_SKIP,
            FAIL_FAST,
            SKIP_AND_LOG
        }
    }

    @Data
    public static class OnFailure {
        private String action;
        private List<String> recipients;
        private Boolean includeStackTrace;
    }

    @Data
    public static class DeadLetterQueue {
        private Boolean enabled;
        private String location;
        private Integer retention;
        private String format;
    }

    @Data
    public static class Monitoring {
        private Boolean metricsEnabled;
        private String metricsPrefix;
        private List<CustomMetric> customMetrics;
        private Alerting alerting;
    }

    @Data
    public static class CustomMetric {
        private String name;
        private MetricType type;

        public enum MetricType {
            COUNTER,
            GAUGE,
            TIMER,
            HISTOGRAM
        }
    }

    @Data
    public static class Alerting {
        private Boolean enabled;
        private List<AlertRule> rules;
    }

    @Data
    public static class AlertRule {
        private String condition;
        private String severity;
        private String action;
    }

    @Data
    public static class ResourceManagement {
        private String totalCpu;
        private String totalMemory;
        private NodeResourceLimits nodeResourceLimits;
        private AutoScaling autoScaling;
    }

    @Data
    public static class NodeResourceLimits {
        private String maxCpuPerNode;
        private String maxMemoryPerNode;
    }

    @Data
    public static class AutoScaling {
        private Boolean enabled;
        private Integer minNodes;
        private Integer maxNodes;
        private Double scaleUpThreshold;
        private Double scaleDownThreshold;
    }

    @Data
    public static class DAGMetadata {
        private String owner;
        private String department;
        private String costCenter;
        private SLA sla;
        private String documentation;
    }

    @Data
    public static class SLA {
        private Integer maxDuration;
        private Double availability;
    }
}
