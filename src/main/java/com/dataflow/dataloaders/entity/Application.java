package com.dataflow.dataloaders.entity;

import com.dataflow.dataloaders.enums.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Application extends AuditMetaData {
    private String id;
    private String name;
    private String description;
    private String iconId;
    private Visibility visibility;
}