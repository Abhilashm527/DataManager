package com.dataflow.dataloaders.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JdbcTableDefinitionResponse {
    private String tableName;
    private String schemaName;
    private List<ColumnDefinition> columns;
    private String rawDdl;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ColumnDefinition {
        private String columnName;
        private String typeName;
        private int columnSize;
        private boolean isNullable;
        private boolean isPrimaryKey;
        private String defaultValue;
    }
}
