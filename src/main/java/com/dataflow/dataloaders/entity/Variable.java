package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@lombok.EqualsAndHashCode(callSuper = true)
public class Variable extends AuditMetaData {
    private String id;
    private String groupId;
    private String variableKey;
    private String variableValue;
    private Boolean isSecret;
    private String description;
}
