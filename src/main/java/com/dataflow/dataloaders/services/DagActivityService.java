package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dto.JdbcDataPreviewResponse;
import com.dataflow.dataloaders.dto.JdbcSchemaTreeResponse;
import com.dataflow.dataloaders.dto.JdbcTableDefinitionResponse;
import com.dataflow.dataloaders.entity.Connection;
import com.dataflow.dataloaders.entity.Provider;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Slf4j
@Service
public class DagActivityService {

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private EncryptionService encryptionService;

    @Autowired
    private VariableService variableService;

    @Autowired
    private ProviderService providerService;

    @Autowired
    private ObjectMapper objectMapper;

    public List<JdbcSchemaTreeResponse> fetchTables(String connectionId, HttpHeaders headers) {
        log.info("Fetching tables for connection: {}", connectionId);
        Identifier identifier = Identifier.builder().word(connectionId).headers(headers).build();
        Connection connection = connectionService.getConnection(identifier);

        try (java.sql.Connection conn = getJdbcConnection(connection)) {
            DatabaseMetaData metaData = conn.getMetaData();

            // Map<SchemaName, JdbcSchemaTreeResponse>
            Map<String, JdbcSchemaTreeResponse> schemaMap = new TreeMap<>();

            try (ResultSet rs = metaData.getTables(null, null, "%", new String[] { "TABLE", "VIEW" })) {
                while (rs.next()) {
                    String schema = rs.getString("TABLE_SCHEM");
                    if (schema == null)
                        schema = "DEFAULT";

                    String type = rs.getString("TABLE_TYPE");
                    String tableName = rs.getString("TABLE_NAME");

                    JdbcSchemaTreeResponse schemaTree = schemaMap.computeIfAbsent(schema,
                            k -> JdbcSchemaTreeResponse.builder()
                                    .schemaName(k)
                                    .tables(new ArrayList<>())
                                    .views(new ArrayList<>())
                                    .build());

                    if ("VIEW".equalsIgnoreCase(type)) {
                        schemaTree.getViews().add(tableName);
                    } else {
                        schemaTree.getTables().add(tableName);
                    }
                }
            }

            return new ArrayList<>(schemaMap.values());
        } catch (Exception e) {
            log.error("Error fetching tables: {}", e.getMessage(), e);
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION,
                    "Failed to fetch tables: " + e.getMessage());
        }
    }

    /**
     * Preview data from a table or custom query
     */
    public JdbcDataPreviewResponse previewData(String connectionId, String schemaName, String tableName, String query,
            Integer limit,
            HttpHeaders headers) {
        log.info("Previewing data for connection: {}, schema: {}, table: {}, query: {}", connectionId, schemaName,
                tableName, query);
        Identifier identifier = Identifier.builder().word(connectionId).headers(headers).build();
        Connection connection = connectionService.getConnection(identifier);

        int finalLimit = (limit == null || limit > 100) ? 50 : limit;
        String finalQuery = query;

        if (finalQuery == null || finalQuery.isEmpty()) {
            if (tableName == null || tableName.isEmpty()) {
                throw new DataloadersException(ErrorFactory.VALIDATION_ERROR,
                        "Either tableName or query must be provided");
            }
            if (schemaName != null && !schemaName.isEmpty() && !"DEFAULT".equalsIgnoreCase(schemaName)) {
                finalQuery = "SELECT * FROM " + schemaName + "." + tableName;
            } else {
                finalQuery = "SELECT * FROM " + tableName;
            }
        }

        // Basic protection: Append limit if not present (simple check)
        String upperQuery = finalQuery.toUpperCase();
        if (!upperQuery.contains("LIMIT") && !upperQuery.contains("TOP ")) {
            // Provider specific limit handling could be added here
            finalQuery += " LIMIT " + finalLimit;
        }

        try (java.sql.Connection conn = getJdbcConnection(connection);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(finalQuery)) {

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                columns.add(rsmd.getColumnName(i));
            }

            List<Map<String, Object>> data = new ArrayList<>();
            int count = 0;
            while (rs.next() && count < finalLimit) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(columns.get(i - 1), rs.getObject(i));
                }
                data.add(row);
                count++;
            }

            return JdbcDataPreviewResponse.builder()
                    .columns(columns)
                    .data(data)
                    .query(finalQuery)
                    .build();
        } catch (Exception e) {
            log.error("Error previewing data: {}", e.getMessage(), e);
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION,
                    "Failed to preview data: " + e.getMessage());
        }
    }

    /**
     * Get detailed definition for a specific table
     */
    public JdbcTableDefinitionResponse getTableDefinition(String connectionId, String schemaName, String tableName,
            HttpHeaders headers) {
        log.info("Getting definition for table: {}.{} in connection: {}", schemaName, tableName, connectionId);
        Identifier identifier = Identifier.builder().word(connectionId).headers(headers).build();
        Connection connection = connectionService.getConnection(identifier);

        String effectiveSchema = (schemaName == null || "DEFAULT".equalsIgnoreCase(schemaName)) ? null : schemaName;

        try (java.sql.Connection conn = getJdbcConnection(connection)) {
            DatabaseMetaData metaData = conn.getMetaData();

            // Get primary keys
            Set<String> primaryKeys = new HashSet<>();
            try (ResultSet rs = metaData.getPrimaryKeys(null, effectiveSchema, tableName)) {
                while (rs.next()) {
                    primaryKeys.add(rs.getString("COLUMN_NAME"));
                }
            }

            // Get columns
            List<JdbcTableDefinitionResponse.ColumnDefinition> columns = new ArrayList<>();
            String actualSchema = schemaName;
            try (ResultSet rs = metaData.getColumns(null, effectiveSchema, tableName, null)) {
                while (rs.next()) {
                    if (actualSchema == null || "DEFAULT".equalsIgnoreCase(actualSchema)) {
                        actualSchema = rs.getString("TABLE_SCHEM");
                    }

                    String colName = rs.getString("COLUMN_NAME");
                    columns.add(JdbcTableDefinitionResponse.ColumnDefinition.builder()
                            .columnName(colName)
                            .typeName(rs.getString("TYPE_NAME"))
                            .columnSize(rs.getInt("COLUMN_SIZE"))
                            .isNullable("YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")))
                            .isPrimaryKey(primaryKeys.contains(colName))
                            .defaultValue(rs.getString("COLUMN_DEF"))
                            .build());
                }
            }

            if (actualSchema == null)
                actualSchema = "DEFAULT";

            String rawDdl = generateRawDdl(tableName, columns);

            return JdbcTableDefinitionResponse.builder()
                    .tableName(tableName)
                    .schemaName(schemaName)
                    .columns(columns)
                    .rawDdl(rawDdl)
                    .build();
        } catch (Exception e) {
            log.error("Error getting table definition: {}", e.getMessage(), e);
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION,
                    "Failed to get table definition: " + e.getMessage());
        }
    }

    private String generateRawDdl(String tableName, List<JdbcTableDefinitionResponse.ColumnDefinition> columns) {
        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(tableName).append(" (\n");
        for (int i = 0; i < columns.size(); i++) {
            JdbcTableDefinitionResponse.ColumnDefinition col = columns.get(i);
            sb.append("  ").append(col.getColumnName()).append(" ").append(col.getTypeName());
            if (col.getColumnSize() > 0 && !col.getTypeName().toLowerCase().contains("date")
                    && !col.getTypeName().toLowerCase().contains("time")) {
                sb.append("(").append(col.getColumnSize()).append(")");
            }
            if (!col.isNullable()) {
                sb.append(" NOT NULL");
            }
            if (i < columns.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(");");
        return sb.toString();
    }

    /**
     * Reuses logic from ConnectionTestService to build a live JDBC connection
     */
    private java.sql.Connection getJdbcConnection(Connection connection) throws Exception {
        Provider provider = providerService.getProvider(Identifier.builder().word(connection.getProviderId()).build());
        String providerKey = provider.getProviderName().toLowerCase();

        // Decrypt secrets
        JsonNode decryptedSecrets = encryptionService.decrypt(connection.getSecrets());

        // Resolve variables
        JsonNode resolvedConfig = variableService.resolveJsonNode(connection.getConfig(), connection.getApplicationId(),
                null);
        JsonNode resolvedSecrets = variableService.resolveJsonNode(decryptedSecrets, connection.getApplicationId(),
                null);

        String url;
        Properties props = new Properties();
        String driverClass;

        if (providerKey.contains("postgres")) {
            String host = resolvedConfig.get("host").asText();
            int port = resolvedConfig.get("port").asInt();
            String db = resolvedConfig.get("database_name").asText();
            url = String.format("jdbc:postgresql://%s:%d/%s", host, port, db);
            props.setProperty("user", resolvedConfig.get("username").asText());
            props.setProperty("password", resolvedSecrets.get("password").asText());
            driverClass = "org.postgresql.Driver";
        } else if (providerKey.contains("mysql") || providerKey.contains("mariadb")) {
            String host = resolvedConfig.get("host").asText();
            int port = resolvedConfig.get("port").asInt();
            String db = resolvedConfig.get("database_name").asText();
            url = String.format("jdbc:mysql://%s:%d/%s?serverTimezone=UTC", host, port, db);
            props.setProperty("user", resolvedConfig.get("username").asText());
            props.setProperty("password", resolvedSecrets.get("password").asText());
            driverClass = "com.mysql.cj.jdbc.Driver";
        } else if (providerKey.contains("oracle")) {
            String host = resolvedConfig.get("host").asText();
            int port = resolvedConfig.get("port").asInt();
            String serviceName = resolvedConfig.get("service_name").asText();
            url = String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, serviceName);
            props.setProperty("user", resolvedConfig.get("username").asText());
            props.setProperty("password", resolvedSecrets.get("password").asText());
            driverClass = "oracle.jdbc.driver.OracleDriver";
        } else if (providerKey.contains("mssql")) {
            String host = resolvedConfig.get("host").asText();
            int port = resolvedConfig.get("port").asInt();
            String db = resolvedConfig.get("database_name").asText();
            url = String.format("jdbc:sqlserver://%s:%d;databaseName=%s", host, port, db);
            props.setProperty("user", resolvedConfig.get("username").asText());
            props.setProperty("password", resolvedSecrets.get("password").asText());
            props.setProperty("trustServerCertificate", "true");
            driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        } else {
            throw new DataloadersException(ErrorFactory.NOT_SUPPORTED,
                    "Provider " + providerKey + " not supported for active inspection");
        }

        Class.forName(driverClass);
        if (connection.getConnectionTimeout() != null) {
            DriverManager.setLoginTimeout(connection.getConnectionTimeout());
        }

        return DriverManager.getConnection(url, props);
    }
}
