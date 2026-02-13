package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@lombok.EqualsAndHashCode(callSuper = true)
public class VariableGroup extends AuditMetaData {
    private String id;
    private String name;
    private String applicationId;
    private String environment;
    private String description;
    private String tags;
    private String groupColor;

    // Virtual field for children
    private List<Variable> variables;
}
