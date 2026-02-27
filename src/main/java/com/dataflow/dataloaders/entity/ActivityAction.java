package com.dataflow.dataloaders.entity;

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
public class ActivityAction {

    // Unique identifier for the action (e.g. "VIEW_DDL", "PREVIEW_DATA")
    private String actionId;

    // UI Label for the button/trigger
    private String label;

    // Optional icon
    private String icon;

    // How the frontend should display the result: "modal", "data_grid", "toast"
    private String type;

    // HTTP Method: "GET", "POST", etc.
    private String method;

    // The endpoint to call (can contain macros like {{connectionId}})
    private String endpoint;

    // A payload template for POST requests (values can be macros)
    private Map<String, Object> payloadTemplate;

    // Which fields in the form must be filled out before this action is unlocked
    private List<String> dependsOn;
}
