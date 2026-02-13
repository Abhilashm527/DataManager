package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.VariableDao;
import com.dataflow.dataloaders.dao.VariableGroupDao;
import com.dataflow.dataloaders.entity.Variable;
import com.dataflow.dataloaders.entity.VariableGroup;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class VariableService {

    @Autowired
    private VariableGroupDao groupDao;

    @Autowired
    private VariableDao variableDao;

    @Autowired
    private EncryptionService encryptionService;

    // --- Group Operations ---

    public VariableGroup createGroup(VariableGroup group, Identifier identifier) {
        log.info("Creating variable group: {}", group.getName());
        return groupDao.createV1(group, identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to create group"));
    }

    public List<VariableGroup> getVariableContext(String applicationId, String environment) {
        log.info("Fetching variables context for App: {}, Env: {}", applicationId, environment);
        List<VariableGroup> groups = groupDao.listByContext(applicationId, environment);
        for (VariableGroup group : groups) {
            List<Variable> variables = variableDao.listByGroup(group.getId());
            maskSecrets(variables);
            group.setVariables(variables);
        }
        return groups;
    }

    public VariableGroup updateGroup(VariableGroup group) {
        groupDao.updateGroup(group);
        return groupDao.getV1(Identifier.builder().word(group.getId()).build()).orElse(null);
    }

    public void deleteGroup(String groupId, String user) {
        VariableGroup group = new VariableGroup();
        group.setId(groupId);
        group.setUpdatedBy(user);
        groupDao.delete(group);
    }

    // --- Variable Operations ---

    public Variable createVariable(Variable variable, Identifier identifier) {
        log.info("Creating variable: {}", variable.getVariableKey());
        if (Boolean.TRUE.equals(variable.getIsSecret()) && variable.getVariableValue() != null) {
            variable.setVariableValue(encryptionService.encryptString(variable.getVariableValue()));
        }
        return variableDao.createV1(variable, identifier)
                .orElseThrow(
                        () -> new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to create variable"));
    }

    public Variable updateVariable(Variable variable) {
        log.info("Updating variable: {}", variable.getId());
        if (Boolean.TRUE.equals(variable.getIsSecret()) && variable.getVariableValue() != null
                && !variable.getVariableValue().equals("...........")) {
            variable.setVariableValue(encryptionService.encryptString(variable.getVariableValue()));
        } else if (Boolean.TRUE.equals(variable.getIsSecret()) && "...........".equals(variable.getVariableValue())) {
            // If the value is the mask, don't update the actual value in DB
            Variable existing = variableDao.getV1(Identifier.builder().word(variable.getId()).build())
                    .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
            variable.setVariableValue(existing.getVariableValue());
        }
        variableDao.updateVariable(variable);
        return variableDao.getV1(Identifier.builder().word(variable.getId()).build()).orElse(null);
    }

    public void deleteVariable(String variableId, String user) {
        Variable var = new Variable();
        var.setId(variableId);
        var.setUpdatedBy(user);
        variableDao.delete(var);
    }

    // --- Helpers ---

    private void maskSecrets(List<Variable> variables) {
        for (Variable var : variables) {
            if (Boolean.TRUE.equals(var.getIsSecret())) {
                var.setVariableValue("..........."); // Mask for UI
            }
        }
    }

    public JsonNode resolveJsonNode(JsonNode node, String applicationId, String environment) {
        if (node == null)
            return null;

        if (node.isObject()) {
            com.fasterxml.jackson.databind.node.ObjectNode objectNode = (com.fasterxml.jackson.databind.node.ObjectNode) node;
            java.util.Iterator<java.util.Map.Entry<String, JsonNode>> fields = objectNode.fields();
            while (fields.hasNext()) {
                java.util.Map.Entry<String, JsonNode> entry = fields.next();
                objectNode.set(entry.getKey(), resolveJsonNode(entry.getValue(), applicationId, environment));
            }
        } else if (node.isArray()) {
            com.fasterxml.jackson.databind.node.ArrayNode arrayNode = (com.fasterxml.jackson.databind.node.ArrayNode) node;
            for (int i = 0; i < arrayNode.size(); i++) {
                arrayNode.set(i, resolveJsonNode(arrayNode.get(i), applicationId, environment));
            }
        } else if (node.isTextual()) {
            String resolvedValue = resolveValue(node.asText(), applicationId, environment);
            return com.fasterxml.jackson.databind.node.TextNode.valueOf(resolvedValue);
        }
        return node;
    }

    public String resolveValue(String rawValue, String applicationId, String environment) {
        if (rawValue == null || !rawValue.contains("{{vars.")) {
            return rawValue;
        }

        // Regex to find all {{vars.GroupName.VariableKey}}
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\{\\{vars\\.([^\\.]+)\\.([^\\}]+)\\}\\}");
        java.util.regex.Matcher matcher = pattern.matcher(rawValue);

        StringBuilder sb = new StringBuilder();
        int lastEnd = 0;

        // Lazy load groups for this context to avoid repeated DB hits
        List<VariableGroup> groups = null;

        while (matcher.find()) {
            sb.append(rawValue, lastEnd, matcher.start());
            String groupName = matcher.group(1);
            String variableKey = matcher.group(2);

            if (groups == null) {
                groups = groupDao.listByContext(applicationId, environment);
            }

            String resolved = findVariableValue(groups, groupName, variableKey);
            sb.append(resolved != null ? resolved : matcher.group(0));
            lastEnd = matcher.end();
        }
        sb.append(rawValue.substring(lastEnd));

        return sb.toString();
    }

    private String findVariableValue(List<VariableGroup> groups, String groupName, String variableKey) {
        for (VariableGroup group : groups) {
            if (group.getName().equalsIgnoreCase(groupName)) {
                List<Variable> variables = variableDao.listByGroup(group.getId());
                for (Variable var : variables) {
                    if (var.getVariableKey().equalsIgnoreCase(variableKey)) {
                        if (Boolean.TRUE.equals(var.getIsSecret())) {
                            return encryptionService.decryptString(var.getVariableValue());
                        }
                        return var.getVariableValue();
                    }
                }
            }
        }
        return null;
    }
}
