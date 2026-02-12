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
    private Long lastTestedAt;
    private Long lastUsedAt;
    private Boolean isFavorite;
}
