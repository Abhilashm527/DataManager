package com.dataflow.dataloaders.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Item extends AuditMetaData{
    private String id;
    private String parentId;
    private String parentFolderId;
    private String name;
    private String type;
    private ItemType itemType; // e.g., app , resources
    private String path;
    private String itemReference;
    private Boolean deletable = true;
    private Boolean active = true;
    private Boolean root;
    private String version = "0.0.1";
    private List<Item> children;
}
