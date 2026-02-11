package com.dataflow.dataloaders.entity;

import lombok.Data;

@Data
public class Datatable extends AuditMetaData{
    private String id;
    private String datatableId;
    private String applicationId;
    private Long deletedAt;
}
