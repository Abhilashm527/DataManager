package com.dataflow.dataloaders.entity;

import com.dataflow.dataloaders.enums.Provider;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "Field definition entity")
public class FieldDefinition {

    @Schema(description = "Unique identifier", example = "Xkdsancksancsak-ds")
    private String id;

    @Schema(description = "Type name", example = "SQL Server")
    private String typeName;

    @Schema(description = "Provider type")
    private Provider provider;

    @Schema(description = "Description")
    private String description;

    @Schema(description = "Configuration fields")
    private List<ConfigField> configFields;

    @Schema(description = "Display order")
    private Long displayOrder;

    @Schema(description = "Active status")
    private Boolean isActive;

    @Schema(description = "Created by")
    private String createdBy;

    @Schema(description = "Created timestamp")
    private Long createdAt;

    @Schema(description = "Updated by")
    private String updatedBy;

    @Schema(description = "Updated timestamp")
    private Long updatedAt;

    @Schema(description = "Deleted timestamp")
    private Long deletedAt;
}