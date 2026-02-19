package com.dataflow.dataloaders.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JdbcTableResponse {
    private String tableName;
    private String tableType;
    private String schema;
    private String catalog;
}
