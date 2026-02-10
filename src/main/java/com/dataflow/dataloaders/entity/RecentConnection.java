package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RecentConnection {
    private Long id;
    private Long userId;
    private Long connectionId;
    private Long accessedAt;
    private Integer accessCount;
}
