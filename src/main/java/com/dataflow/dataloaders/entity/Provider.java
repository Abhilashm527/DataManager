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
public class Provider extends AuditMetaData {
    private String id;
    private String connectionTypeId;
    private String providerName;
    private String iconId;
    private Integer defaultPort;
    private JsonNode configSchema;
    private Integer displayOrder;
}
