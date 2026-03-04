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
public class Dataflow extends AuditMetaData {
    private String id;
    private String applicationId;
    private String dataflowName;
    private String description;
    private Boolean isActive;
    private Boolean isFavorite;

    @com.fasterxml.jackson.annotation.JsonIgnore
    private Long total;
}
