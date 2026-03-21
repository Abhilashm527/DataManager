package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dto.FieldAutoMapRequest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Service
public class GeminiService {

    @Value("${gemini.api-key:}")
    private String apiKey;

    @Value("${gemini.url:https://generativelanguage.googleapis.com/v1beta}")
    private String geminiUrl;

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Calls Gemini to auto-map source fields to target fields by name/type similarity.
     * Returns a list of {sourcePath, targetPath} mappings.
     */
    public List<Map<String, String>> autoMapFields(List<FieldAutoMapRequest.FieldEntry> sourceFields,
                                                   List<FieldAutoMapRequest.FieldEntry> targetFields) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("Gemini API key is not configured (GEMINI_API_KEY)");
        }

        String prompt = String.format(
                "Map source fields to target fields by name and type similarity.\n" +
                "Source fields: %s\n" +
                "Target fields: %s\n" +
                "Return ONLY a JSON array with no explanation: [{\"sourcePath\":\"...\",\"targetPath\":\"...\"}]",
                toJson(sourceFields), toJson(targetFields)
        );

        String url = geminiUrl + "/models/gemini-2.0-flash:generateContent?key=" + apiKey;

        Map<String, Object> body = Map.of(
                "contents", List.of(Map.of("parts", List.of(Map.of("text", prompt)))),
                "generationConfig", Map.of("temperature", 0.1)
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);

        return parseMappings(response.getBody(), sourceFields, targetFields);
    }

    private List<Map<String, String>> parseMappings(String responseBody,
                                                     List<FieldAutoMapRequest.FieldEntry> sourceFields,
                                                     List<FieldAutoMapRequest.FieldEntry> targetFields) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);
            String text = root.path("candidates").get(0)
                    .path("content").path("parts").get(0)
                    .path("text").asText();

            // Extract JSON array from the text
            Pattern pattern = Pattern.compile("\\[.*?\\]", Pattern.DOTALL);
            Matcher matcher = pattern.matcher(text);
            if (!matcher.find()) {
                log.warn("Gemini response contained no JSON array: {}", text);
                return Collections.emptyList();
            }

            List<Map<String, String>> raw = objectMapper.readValue(
                    matcher.group(), new TypeReference<>() {});

            Set<String> srcNames = new HashSet<>();
            sourceFields.forEach(f -> srcNames.add(f.getName()));
            Set<String> tgtNames = new HashSet<>();
            targetFields.forEach(f -> tgtNames.add(f.getName()));

            // Keep only valid mappings referencing actual field names
            return raw.stream()
                    .filter(m -> srcNames.contains(m.get("sourcePath")) && tgtNames.contains(m.get("targetPath")))
                    .toList();

        } catch (Exception e) {
            log.error("Failed to parse Gemini response: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to parse Gemini auto-map response: " + e.getMessage());
        }
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return obj.toString();
        }
    }
}
