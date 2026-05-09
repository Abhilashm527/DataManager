package com.dataflow.dataloaders.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class GenerateSqlQueryRequest {

    private String tableName;
    private String schemaName;
    private String connectionId;
    private List<Map<String, String>> selectedFields;
    private List<Map<String, String>> allColumns;
}
