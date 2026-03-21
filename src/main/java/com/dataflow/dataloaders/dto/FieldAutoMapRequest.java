package com.dataflow.dataloaders.dto;

import lombok.Data;

import java.util.List;

@Data
public class FieldAutoMapRequest {

    private List<FieldEntry> sourceFields;
    private List<FieldEntry> targetFields;

    @Data
    public static class FieldEntry {
        private String name;
        private String type;
    }
}
