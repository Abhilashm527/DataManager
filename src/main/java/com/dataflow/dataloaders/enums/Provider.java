package com.dataflow.dataloaders.enums;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Provider type enum")
public enum Provider {
    @Schema(description = "Connection provider")
    CONNECTION,
    
    @Schema(description = "Source provider")
    SOURCE,
    
    @Schema(description = "Target provider")
    TARGET
}