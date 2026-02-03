package com.dataflow.dataloaders.util;

import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.util.Map;

@Component
@Slf4j
public class DFUtil {

    @Autowired
    private ObjectMapper objectMapper;

    public String writeValueAsString(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new DataloadersException(ErrorFactory.JSON_PROCESSING_ERROR);
        }
    }

    public <T> T readValue(Class<T> type, String value) {
        try {
            return objectMapper.readValue(value, type);
        } catch (JsonMappingException e) {
            throw new DataloadersException(ErrorFactory.JSON_PROCESSING_ERROR);
        } catch (JsonProcessingException e) {
            throw new DataloadersException(ErrorFactory.JSON_PROCESSING_ERROR);
        }
    }

    public <T> T readValue(TypeReference<T> typeReference, String value) {
        try {
            return objectMapper.readValue(value, typeReference);
        } catch (JsonMappingException e) {
            throw new DataloadersException(ErrorFactory.JSON_PROCESSING_ERROR);
        } catch (JsonProcessingException e) {
            throw new DataloadersException(ErrorFactory.JSON_PROCESSING_ERROR);
        }
    }

    public Map<String, Object> readValueToMap(String value) {
        try {
            return objectMapper.readValue(value, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException e) {
            throw new DataloadersException(ErrorFactory.JSON_PROCESSING_ERROR);
        }
    }

    public Map<String, Map<String, Object>> readValueToMapofMap(String value) {
        try {
            return objectMapper.readValue(value, new TypeReference<Map<String, Map<String, Object>>>() {
            });
        } catch (JsonProcessingException e) {
            throw new DataloadersException(ErrorFactory.JSON_PROCESSING_ERROR);
        }
    }

    private String buildUrl(String base, Map<String, String> pathVars, Map<String, String> queryParams) {
        StringBuilder sb = new StringBuilder(base);

        if (!ObjectUtils.isEmpty(pathVars)) {
            pathVars.forEach((k, v) -> {
                String placeholder = "{" + k + "}";
                int idx;
                while ((idx = sb.indexOf(placeholder)) != -1) {
                    sb.replace(idx, idx + placeholder.length(), v);
                }
            });
        }

        if (!ObjectUtils.isEmpty(queryParams)) {
            sb.append("?");
            queryParams.forEach((k, v) -> sb.append(k).append("=").append(v).append("&"));
            sb.setLength(sb.length() - 1); // remove trailing &
        }

        return sb.toString();
    }

    public static String toSnakeCase(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        // Insert underscore before each uppercase letter (except the first), and convert to lowercase
        return input.replaceAll("([a-z])([A-Z]+)", "$1_$2")
                .replaceAll("([A-Z])([A-Z][a-z])", "$1_$2")
                .toLowerCase();
    }

    public <T> T readValue(String json, TypeReference<T> typeReference) {
        try {
            return objectMapper.readValue(json, typeReference);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON string: " + json, e);
        }
    }

}