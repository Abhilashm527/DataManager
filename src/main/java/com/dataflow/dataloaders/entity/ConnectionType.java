package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionType extends AuditMetaData {
    private String id;
    private String connectionType;
    private String iconId;
    private Integer displayOrder;
}
