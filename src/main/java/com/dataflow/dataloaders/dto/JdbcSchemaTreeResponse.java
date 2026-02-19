package com.dataflow.dataloaders.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JdbcSchemaTreeResponse {
    private String schemaName;
    @Builder.Default
    private List<String> tables = new ArrayList<>();
    @Builder.Default
    private List<String> views = new ArrayList<>();
}
