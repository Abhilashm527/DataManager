package com.dataflow.dataloaders.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConnectionStringParseResponse {
    private boolean valid;
    private String providerKey;
    private String host;
    private Integer port;
    private String database;
    private String username;
    private String password;          // always "****" when present
    private Boolean useSsl;
    private Map<String, String> additionalParams;
    private String errorMessage;
}
