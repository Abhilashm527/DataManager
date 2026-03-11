package com.dataflow.dataloaders.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Datatable extends AuditMetaData {
    private String id;
    private String applicationId;
    private String tableName;
    private String description;
    private String status;
    private List<ColumnDefinition> columns;
    private Map<String, Object> metadata;
    @com.fasterxml.jackson.annotation.JsonProperty("total")
    private Long total;
    @com.fasterxml.jackson.annotation.JsonProperty("recordCount")
    private Long recordCount;
    @com.fasterxml.jackson.annotation.JsonProperty("fieldCount")
    private Integer fieldCount;

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ColumnDefinition {
        private String name;
        private String dataType;
        private boolean required;
        private Object defaultValue;
        private String description;
        private boolean isIndexed;
        private boolean isUnique;
        private Map<String, Object> constraints;
    }
}
