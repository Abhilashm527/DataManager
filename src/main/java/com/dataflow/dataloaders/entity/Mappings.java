package com.dataflow.dataloaders.entity;

import com.dataflow.dataloaders.jobconfigs.InputField;
import lombok.Data;

import java.util.List;

@Data
public class Mappings extends AuditMetaData{
    public String id;
    public String mappingName;
    public List<InputField> mappings;
    private Boolean isActive;
    private String itemId;
    private Long deletedAt;
}
