package com.dataflow.dataloaders.entity;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionActivityLog {
    private Long id;
    private Long connectionId;
    private String activityType;
    private String status;
    private String title;
    private String description;
    private JsonNode metadata;
    private Long createdAt;
}
