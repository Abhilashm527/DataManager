package com.dataflow.dataloaders.entity;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@lombok.EqualsAndHashCode(callSuper = true)
public class Connection extends AuditMetaData {
    private String id;
    private String applicationId;
    private String providerId;
    private String connectionName;
    private JsonNode config;
    private JsonNode secrets;
    private Boolean useSsl;
    private Integer connectionTimeout;
    private Boolean isActive;
    private String lastTestStatus;
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Long lastTestedAt;
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Long lastUsedAt;
    private Boolean isFavorite;

    @com.fasterxml.jackson.annotation.JsonIgnore
    private Long total;

    @com.fasterxml.jackson.annotation.JsonProperty("lastTestedAtDisplay")
    public String getLastTestedAtDisplay() {
        return (lastTestedAt != null) ? com.dataflow.dataloaders.util.DateUtils.getFormattedDate(lastTestedAt) : null;
    }

    @com.fasterxml.jackson.annotation.JsonProperty("lastUsedAtDisplay")
    public String getLastUsedAtDisplay() {
        return (lastUsedAt != null) ? com.dataflow.dataloaders.util.DateUtils.getFormattedDate(lastUsedAt) : null;
    }
}
