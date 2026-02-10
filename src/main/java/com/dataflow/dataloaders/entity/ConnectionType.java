package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionType {
    private Long id;
    private String typeKey;
    private String displayName;
    private Long iconId;
    private Integer displayOrder;
    private Long createdAt;
}
