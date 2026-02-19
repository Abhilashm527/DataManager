package com.dataflow.dataloaders.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JdbcDataPreviewResponse {
    private List<String> columns;
    private List<Map<String, Object>> data;
    private String query;
}
