package com.dataflow.dataloaders.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.util.Map;

@Data
@Document(collection = "data_table_records")
public class DataTableRecord {
    @Id
    private String id;
    private String tableId; // This links to Datatable.datatableId or Datatable.id
    private Map<String, Object> data;
    private String searchText;
    private Long createdAt;
    private Long updatedAt;
    private Map<String, Object> metadata;
}
