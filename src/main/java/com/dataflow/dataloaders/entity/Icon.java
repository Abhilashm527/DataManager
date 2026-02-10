package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Icon {
    private Long id;
    private String iconName;
    private String iconUrl;
    private byte[] iconData;
    private String contentType;
    private Long fileSize;
    private Long createdAt;
}
