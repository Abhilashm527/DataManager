package com.dataflow.dataloaders.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RecentConnectionResponse {
    private Long id;
    private Long userId;
    private Long connectionId;
    private String connectionName;
    private String providerDisplayName;
    private Long accessedAt;
    private Integer accessCount;
}