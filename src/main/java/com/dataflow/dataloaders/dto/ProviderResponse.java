package com.dataflow.dataloaders.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProviderResponse {
    private Long id;
    private Long connectionTypeId;
    private String providerKey;
    private String displayName;
    private Long iconId;
    private Integer defaultPort;
    private JsonNode configSchema;
    private Integer displayOrder;
    private Long createdAt;
}
