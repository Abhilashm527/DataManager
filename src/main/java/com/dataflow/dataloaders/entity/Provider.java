package com.dataflow.dataloaders.entity;

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
    private String description;
    private String iconId;
    private Integer defaultPort;
    private ConfigSchema configSchema;
    private Integer displayOrder;
}
