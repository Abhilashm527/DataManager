package com.dataflow.dataloaders.validator;

import com.dataflow.dataloaders.entity.dagmodels.dag.Node;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.jobconfigs.ReaderConfig;
import com.dataflow.dataloaders.jobconfigs.WriterConfig;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;

@Component
public class NodeValidator {

    public void validate(Node node) {
        if (node == null) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, "Node cannot be null");
        }

        if (node.getDataflowId() == null || node.getDataflowId().trim().isEmpty()) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, "Dataflow ID is required for a node");
        }

        if (node.getNodeName() == null || node.getNodeName().trim().isEmpty()) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, "Node name is required");
        }

        if (node.getNodeType() == null) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST,
                    String.format("Node type is required for node: %s", node.getNodeName()));
        }

        switch (node.getNodeType()) {
            case JDBC_READER:
                validateJdbcReader(node);
                break;
            case JDBC_WRITER:
                validateJdbcWriter(node);
                break;
            // Add more cases as needed for other NodeType values
            default:
                break;
        }
    }

    private void validateJdbcReader(Node node) {
        Map<String, Object> fields = getConfigFields(node, true);
        if (!hasField(fields, "connectionName") && !hasField(fields, "connectionId")) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST,
                    String.format("JDBC Reader node '%s' requires connectionName or connectionId", node.getNodeName()));
        }

        boolean hasTable = hasField(fields, "tableName");
        boolean hasSelect = hasField(fields, "selectClause");
        boolean hasFrom = hasField(fields, "fromClause");

        if (!hasTable && !(hasSelect && hasFrom)) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST,
                    String.format(
                            "JDBC Reader node '%s' requires either 'tableName' or both 'selectClause' and 'fromClause'",
                            node.getNodeName()));
        }
    }

    private void validateJdbcWriter(Node node) {
        Map<String, Object> fields = getConfigFields(node, false);
        if (!hasField(fields, "connectionName") && !hasField(fields, "connectionId")) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST,
                    String.format("JDBC Writer node '%s' requires connectionName or connectionId", node.getNodeName()));
        }
        validateRequired(fields, "tableName",
                String.format("JDBC Writer node '%s' requires tableName", node.getNodeName()));
    }

    private Map<String, Object> getConfigFields(Node node, boolean isSource) {
        if (node.getConfig() == null) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST,
                    String.format("Configuration is missing for node: %s", node.getNodeName()));
        }

        if (isSource) {
            if (node.getConfig().getReaderConfig() != null) {
                Map<String, Object> fields = new HashMap<>();
                ReaderConfig rc = node.getConfig().getReaderConfig();
                fields.put("connectionId", rc.getConnectionId());
                fields.put("connectionName", rc.getConnectionName());
                fields.put("tableName", rc.getTableName());
                fields.put("schemaName", rc.getSchemaName());
                fields.put("selectClause", rc.getSelectClause());
                fields.put("fromClause", rc.getFromClause());
                return fields;
            }

            return null;
        } else {
            if (node.getConfig().getWriterConfig() != null) {
                Map<String, Object> fields = new HashMap<>();
                WriterConfig wc = node.getConfig().getWriterConfig();
                fields.put("connectionId", wc.getConnectionId());
                fields.put("tableName", wc.getTableName());
                fields.put("schemaName", wc.getSchemaName());
                return fields;
            }
            return null;
        }
    }

    private void validateRequired(Map<String, Object> fields, String fieldName, String errorMessage) {
        if (!hasField(fields, fieldName)) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, errorMessage);
        }
    }

    private boolean hasField(Map<String, Object> fields, String fieldName) {
        Object value = fields.get(fieldName);
        return value != null && !value.toString().trim().isEmpty();
    }
}
