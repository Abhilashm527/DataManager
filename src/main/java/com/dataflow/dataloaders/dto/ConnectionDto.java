package com.dataflow.dataloaders.dto;

import com.dataflow.dataloaders.entity.Connection;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ConnectionDto extends Connection {
    private String providerName;
    private String icon;
}
