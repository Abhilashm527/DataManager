package com.dataflow.dataloaders.entity;

import com.dataflow.dataloaders.util.DateUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
public class AuditMetaData {
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Long createdAt;
    private String createdBy;
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Long updatedAt;
    private String updatedBy;
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Long deletedAt;

    @JsonProperty("createdAtDisplay")
    public String getCreatedAtDisplay() {
        return (createdAt != null) ? DateUtils.getFormattedDate(createdAt) : null;
    }

    @JsonProperty("updatedAtDisplay")
    public String getUpdatedAtDisplay() {
        return (updatedAt != null) ? DateUtils.getFormattedDate(updatedAt) : null;
    }
}
