package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ConnectionDao;
import com.dataflow.dataloaders.dao.ProviderDao;
import com.dataflow.dataloaders.dto.TestConnectionRequest;
import com.dataflow.dataloaders.dto.TestConnectionResponse;
import com.dataflow.dataloaders.entity.Connection;
import com.dataflow.dataloaders.entity.Provider;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.SocketTimeoutException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
@Service
public class ConnectionTestService {

    @Autowired
    private ConnectionDao connectionDao;

    @Autowired
    private ProviderDao providerDao;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EncryptionService encryptionService;

    /**
     * Test a new connection before saving
     */
    public TestConnectionResponse testNewConnection(TestConnectionRequest request) {
        log.info("Testing new connection for provider: {}", request.getProviderKey());
        long startTime = System.currentTimeMillis();

        try {
            // Skip provider lookup for new connection tests - test directly
            return performConnectionTest(
                    request.getProviderKey(),
                    request.getConfig(),
                    request.getSecrets(),
                    request.getUseSsl(),
                    request.getConnectionTimeout(),
                    startTime,
                    null);

        } catch (Exception e) {
            int responseTime = (int) (System.currentTimeMillis() - startTime);
            log.error("Error while testing connection: {}", e.getMessage(), e);
            return buildFailureResponse(responseTime, "ERROR", e.getMessage(),
                    Arrays.asList("Verify the connection details", "Check server availability"));
        }
    }

    /**
     * Test an existing saved connection
     */
    public TestConnectionResponse testExistingConnection(String connectionId, Identifier identifier) {
        log.info("Testing existing connection: {}", connectionId);
        long startTime = System.currentTimeMillis();

        try {
            // Get connection
            Connection connection = connectionDao.getV1(Identifier.builder().word(connectionId).build())
                    .orElseThrow(
                            () -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND, "Connection not found"));

            // Get provider
            Provider provider = providerDao.getV1(Identifier.builder().word(connection.getProviderId()).build())
                    .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND, "Provider not found"));

            // Decrypt secrets
            JsonNode decryptedSecrets = encryptionService.decrypt(connection.getSecrets());

            // Perform connection test
            TestConnectionResponse response = performConnectionTest(
                    provider.getProviderName(),
                    connection.getConfig(),
                    decryptedSecrets,
                    connection.getUseSsl(),
                    connection.getConnectionTimeout(),
                    startTime,
                    connectionId);

            // Update connection status
            connection.setLastTestStatus(response.getSuccess() ? "success" : "failure");
            connection.setLastTestedAt(DateUtils.getUnixTimestampInUTC());
            connection.setLastUsedAt(DateUtils.getUnixTimestampInUTC());
            connectionDao.update(connection);

            return response;

        } catch (Exception e) {
            int responseTime = (int) (System.currentTimeMillis() - startTime);
            log.error("Error testing existing connection {}: {}", connectionId, e.getMessage(), e);
            return buildFailureResponse(responseTime, "ERROR", e.getMessage(),
                    Arrays.asList("Check connection configuration", "Verify server is reachable"));
        }
    }

    /**
     * Validate config fields against provider schema
     */
    private void validateConfigAgainstSchema(JsonNode schema, JsonNode config, JsonNode secrets) {
        if (schema == null || !schema.has("fields")) {
            return;
        }

        JsonNode fields = schema.get("fields");
        for (JsonNode field : fields) {
            String key = field.get("key").asText();
            boolean required = field.has("required") && field.get("required").asBoolean();
            boolean isSecret = field.has("isSecret") && field.get("isSecret").asBoolean();

            if (required) {
                JsonNode source = isSecret ? secrets : config;
                if (source == null || !source.has(key) || source.get(key).isNull()
                        || source.get(key).asText().trim().isEmpty()) {
                    throw new DataloadersException(ErrorFactory.VALIDATION_ERROR, "Missing required field: " + key);
                }
            }
        }
    }

    /**
     * Perform the actual connection test based on provider type
     */
    private TestConnectionResponse performConnectionTest(
            String providerKey,
            JsonNode config,
            JsonNode secrets,
            Boolean useSsl,
            Integer connectionTimeout,
            long startTime,
            String connectionId) {

        try {
            JsonNode serverInfo;

            switch (providerKey.toLowerCase()) {
                case "postgresql":
                    serverInfo = testPostgreSQL(config, secrets, useSsl, connectionTimeout);
                    break;

                case "mysql":
                    serverInfo = testMySQL(config, secrets, useSsl, connectionTimeout);
                    break;

                case "mongodb":
                    serverInfo = testMongoDB(config, secrets, useSsl, connectionTimeout);
                    break;

                case "redis":
                    serverInfo = testRedis(config, secrets, connectionTimeout);
                    break;

                case "oracle":
                    serverInfo = testOracle(config, secrets, useSsl, connectionTimeout);
                    break;

                case "mssql":
                    serverInfo = testMSSQL(config, secrets, useSsl, connectionTimeout);
                    break;

                case "mariadb":
                    serverInfo = testMariaDB(config, secrets, useSsl, connectionTimeout);
                    break;

                default:
                    return buildNotSupportedResponse((int) (System.currentTimeMillis() - startTime), providerKey);
            }

            int responseTime = (int) (System.currentTimeMillis() - startTime);

            return TestConnectionResponse.builder()
                    .success(true)
                    .status("success")
                    .message("Connection successful")
                    .responseTimeMs(responseTime)
                    .serverInfo(serverInfo)
                    .build();

        } catch (SQLTimeoutException | SocketTimeoutException e) {
            int responseTime = (int) (System.currentTimeMillis() - startTime);
            return buildFailureResponse(responseTime, "TIMEOUT",
                    "Connection timeout after " + (connectionTimeout != null ? connectionTimeout : 30) + " seconds",
                    Arrays.asList(
                            "Check if the host is reachable from your network",
                            "Verify firewall settings allow connections on this port",
                            "Ensure the port number is correct",
                            "Try increasing the connection timeout value"));

        } catch (SQLException e) {
            int responseTime = (int) (System.currentTimeMillis() - startTime);
            String errorCode = categorizeSQLException(e);
            return buildFailureResponse(responseTime, errorCode, e.getMessage(),
                    getTroubleshootingForError(errorCode, providerKey));

        } catch (JedisConnectionException e) {
            int responseTime = (int) (System.currentTimeMillis() - startTime);
            String errorCode = "CONNECTION_FAILED";
            if (e.getMessage().contains("NOAUTH") || e.getMessage().contains("authentication")) {
                errorCode = "INVALID_AUTHORIZATION";
            }
            return buildFailureResponse(responseTime, errorCode, e.getMessage(),
                    getTroubleshootingForError(errorCode, "redis"));

        } catch (Exception e) {
            int responseTime = (int) (System.currentTimeMillis() - startTime);
            String errorCode = categorizeException(e);
            return buildFailureResponse(responseTime, errorCode, e.getMessage(),
                    getTroubleshootingForError(errorCode, providerKey));
        }
    }

    // ============================================================================
    // DATABASE-SPECIFIC TEST METHODS
    // ============================================================================

    private JsonNode testPostgreSQL(JsonNode config, JsonNode secrets, Boolean useSsl, Integer connectionTimeout)
            throws Exception {
        String host = config.get("host").asText();
        int port = config.get("port").asInt();
        String database = config.get("database_name").asText();
        String username = config.get("username").asText();
        String password = secrets.get("password").asText();

        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);

        if (useSsl != null && useSsl) {
            props.setProperty("ssl", "true");
            props.setProperty("sslmode", "require");
        }

        if (connectionTimeout != null) {
            props.setProperty("loginTimeout", String.valueOf(connectionTimeout));
            props.setProperty("connectTimeout", String.valueOf(connectionTimeout * 1000));
        }

        Class.forName("org.postgresql.Driver");

        try (java.sql.Connection conn = DriverManager.getConnection(url, props);
                java.sql.Statement stmt = conn.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery("SELECT version(), current_timestamp")) {

            ObjectNode serverInfo = objectMapper.createObjectNode();
            if (rs.next()) {
                serverInfo.put("version", rs.getString(1));
                serverInfo.put("serverTime", rs.getTimestamp(2).toString());
            }
            serverInfo.put("databaseProductName", conn.getMetaData().getDatabaseProductName());
            return serverInfo;
        }
    }

    private JsonNode testMySQL(JsonNode config, JsonNode secrets, Boolean useSsl, Integer connectionTimeout)
            throws Exception {
        String host = config.get("host").asText();
        int port = config.get("port").asInt();
        String database = config.get("database_name").asText();
        String username = config.get("username").asText();
        String password = secrets.get("password").asText();

        String url = String.format("jdbc:mysql://%s:%d/%s?serverTimezone=UTC", host, port, database);
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);

        if (useSsl != null && useSsl) {
            props.setProperty("useSSL", "true");
            props.setProperty("requireSSL", "true");
        } else {
            props.setProperty("useSSL", "false");
        }

        if (connectionTimeout != null) {
            props.setProperty("connectTimeout", String.valueOf(connectionTimeout * 1000));
            props.setProperty("socketTimeout", String.valueOf(connectionTimeout * 1000));
        }

        Class.forName("com.mysql.cj.jdbc.Driver");

        try (java.sql.Connection conn = DriverManager.getConnection(url, props);
                java.sql.Statement stmt = conn.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery("SELECT VERSION(), NOW()")) {

            ObjectNode serverInfo = objectMapper.createObjectNode();
            if (rs.next()) {
                serverInfo.put("version", rs.getString(1));
                serverInfo.put("serverTime", rs.getTimestamp(2).toString());
            }
            serverInfo.put("databaseProductName", conn.getMetaData().getDatabaseProductName());
            return serverInfo;
        }
    }

    private JsonNode testMongoDB(JsonNode config, JsonNode secrets, Boolean useSsl, Integer connectionTimeout)
            throws Exception {
        String host = config.get("host").asText();
        int port = config.has("port") ? config.get("port").asInt() : 27017;
        String database = config.get("database_name").asText();

        String uri;
        if (config.has("connection_string") && !config.get("connection_string").asText().isEmpty()) {
            uri = config.get("connection_string").asText();
        } else {
            String username = config.has("username") ? config.get("username").asText() : null;
            String password = secrets.has("password") ? secrets.get("password").asText() : null;

            if (username != null && password != null) {
                uri = String.format("mongodb://%s:%s@%s:%d/%s", username, password, host, port, database);
            } else {
                uri = String.format("mongodb://%s:%d/%s", host, port, database);
            }

            if (useSsl != null && useSsl) {
                uri += "?ssl=true";
            }
        }

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase db = mongoClient.getDatabase(database);

            Document pingCommand = new Document("ping", 1);
            db.runCommand(pingCommand);

            Document buildInfo = db.runCommand(new Document("buildInfo", 1));

            ObjectNode serverInfo = objectMapper.createObjectNode();
            serverInfo.put("version", buildInfo.getString("version"));
            serverInfo.put("serverTime", DateUtils.getUnixTimestampInUTC());
            serverInfo.put("databaseProductName", "MongoDB");

            return serverInfo;
        }
    }

    private JsonNode testRedis(JsonNode config, JsonNode secrets, Integer connectionTimeout) throws Exception {
        String host = config.get("host").asText();
        int port = config.get("port").asInt();
        String password = secrets.has("password") ? secrets.get("password").asText() : null;
        int database = config.has("database") ? config.get("database").asInt() : 0;

        try (Jedis jedis = new Jedis(host, port, connectionTimeout != null ? connectionTimeout * 1000 : 30000)) {
            if (password != null && !password.isEmpty()) {
                jedis.auth(password);
            }

            jedis.select(database);
            String pong = jedis.ping();

            if (!"PONG".equals(pong)) {
                throw new Exception("Redis ping failed");
            }

            String info = jedis.info("server");
            String version = extractRedisVersion(info);

            ObjectNode serverInfo = objectMapper.createObjectNode();
            serverInfo.put("version", "Redis " + version);
            serverInfo.put("serverTime", DateUtils.getUnixTimestampInUTC());
            serverInfo.put("databaseProductName", "Redis");

            return serverInfo;
        }
    }

    private JsonNode testOracle(JsonNode config, JsonNode secrets, Boolean useSsl, Integer connectionTimeout)
            throws Exception {
        String host = config.get("host").asText();
        int port = config.get("port").asInt();
        String serviceName = config.get("service_name").asText();
        String username = config.get("username").asText();
        String password = secrets.get("password").asText();

        String url = String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, serviceName);
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);

        if (connectionTimeout != null) {
            props.setProperty("oracle.net.CONNECT_TIMEOUT", String.valueOf(connectionTimeout * 1000));
        }

        Class.forName("oracle.jdbc.driver.OracleDriver");

        try (java.sql.Connection conn = DriverManager.getConnection(url, props);
                java.sql.Statement stmt = conn.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery("SELECT * FROM v$version WHERE banner LIKE 'Oracle%'")) {

            ObjectNode serverInfo = objectMapper.createObjectNode();
            if (rs.next()) {
                serverInfo.put("version", rs.getString(1));
            }
            serverInfo.put("serverTime", DateUtils.getUnixTimestampInUTC());
            serverInfo.put("databaseProductName", conn.getMetaData().getDatabaseProductName());

            return serverInfo;
        }
    }

    private JsonNode testMSSQL(JsonNode config, JsonNode secrets, Boolean useSsl, Integer connectionTimeout)
            throws Exception {
        String host = config.get("host").asText();
        int port = config.get("port").asInt();
        String database = config.get("database_name").asText();
        String username = config.get("username").asText();
        String password = secrets.get("password").asText();

        String url = String.format("jdbc:sqlserver://%s:%d;databaseName=%s", host, port, database);
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("encrypt", useSsl != null && useSsl ? "true" : "false");
        props.setProperty("trustServerCertificate", "true");

        if (connectionTimeout != null) {
            props.setProperty("loginTimeout", String.valueOf(connectionTimeout));
        }

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        try (java.sql.Connection conn = DriverManager.getConnection(url, props);
                java.sql.Statement stmt = conn.createStatement();
                java.sql.ResultSet rs = stmt.executeQuery("SELECT @@VERSION as version, GETDATE() as serverTime")) {

            ObjectNode serverInfo = objectMapper.createObjectNode();
            if (rs.next()) {
                String fullVersion = rs.getString("version");
                serverInfo.put("version", fullVersion.split("\n")[0]);
                serverInfo.put("serverTime", rs.getTimestamp("serverTime").toString());
            }
            serverInfo.put("databaseProductName", conn.getMetaData().getDatabaseProductName());

            return serverInfo;
        }
    }

    private JsonNode testMariaDB(JsonNode config, JsonNode secrets, Boolean useSsl, Integer connectionTimeout)
            throws Exception {
        // MariaDB uses MySQL driver with slight differences
        return testMySQL(config, secrets, useSsl, connectionTimeout);
    }

    // ============================================================================
    // HELPER METHODS
    // ============================================================================

    private String extractRedisVersion(String info) {
        for (String line : info.split("\r\n")) {
            if (line.startsWith("redis_version:")) {
                return line.split(":")[1];
            }
        }
        return "Unknown";
    }

    private String categorizeSQLException(SQLException e) {
        String sqlState = e.getSQLState();
        String message = e.getMessage().toLowerCase();

        // Connection errors
        if (sqlState != null && sqlState.startsWith("08"))
            return "CONNECTION_REFUSED";

        // Authentication errors
        if (message.contains("authentication") || message.contains("password") ||
                message.contains("login") || message.contains("access denied")) {
            return "INVALID_AUTHORIZATION";
        }

        // Database not found
        if (message.contains("database") && (message.contains("not exist") || message.contains("unknown database"))) {
            return "DATABASE_NOT_FOUND";
        }

        // Host unreachable
        if (message.contains("host") || message.contains("unknown") || message.contains("could not connect")) {
            return "HOST_UNREACHABLE";
        }

        // Timeout
        if (message.contains("timeout") || message.contains("timed out")) {
            return "TIMEOUT";
        }

        return "SQL_ERROR";
    }

    private String categorizeException(Exception e) {
        String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";

        if (message.contains("timeout") || message.contains("timed out")) {
            return "TIMEOUT";
        }
        if (message.contains("refused") || message.contains("connection refused")) {
            return "CONNECTION_REFUSED";
        }
        if (message.contains("authentication") || message.contains("auth") || message.contains("password")) {
            return "INVALID_AUTHORIZATION";
        }
        if (message.contains("not found") || message.contains("unknown host")) {
            return "HOST_UNREACHABLE";
        }

        return "CONNECTION_FAILED";
    }

    private List<String> getTroubleshootingForError(String errorCode, String providerKey) {
        switch (errorCode) {
            case "INVALID_AUTHORIZATION":
                if ("mongodb".equals(providerKey)) {
                    return Arrays.asList(
                            "Verify username and password are correct",
                            "Check the authentication database (authSource)",
                            "Ensure the user has the correct roles assigned",
                            "Verify authentication mechanism is supported");
                } else if ("redis".equals(providerKey)) {
                    return Arrays.asList(
                            "Verify the password is correct",
                            "Check if Redis server requires authentication",
                            "Ensure requirepass is configured correctly on server");
                } else {
                    return Arrays.asList(
                            "Verify username and password are correct",
                            "Check if the user has access to the database",
                            "Ensure credentials are not expired",
                            "Verify the user has required permissions");
                }

            case "DATABASE_NOT_FOUND":
                return Arrays.asList(
                        "Verify the database name is correct and case-sensitive",
                        "Check if the database has been created",
                        "Ensure you have access to the specified database",
                        "For MongoDB, the database will be created on first write");

            case "HOST_UNREACHABLE":
                return Arrays.asList(
                        "Check if the hostname is spelled correctly",
                        "Verify DNS resolution is working",
                        "Try using an IP address instead of hostname",
                        "Check your network connection");

            case "CONNECTION_REFUSED":
                return Arrays.asList(
                        "Verify the " + providerKey + " server is running",
                        "Check if the port number is correct",
                        "Ensure firewall allows connections on this port",
                        "Verify the server is configured to accept connections");

            case "TIMEOUT":
                return Arrays.asList(
                        "Check if the host is reachable from your network",
                        "Verify firewall settings allow connections",
                        "Ensure the port number is correct",
                        "Try increasing the connection timeout value");

            case "NOT_SUPPORTED":
                return Arrays.asList(
                        "This database provider is not yet supported",
                        "Contact support for assistance",
                        "Check documentation for supported providers");

            default:
                return Arrays.asList(
                        "Review the error message carefully",
                        "Check server logs for more details",
                        "Verify all connection parameters are correct",
                        "Contact your database administrator if the issue persists");
        }
    }

    private TestConnectionResponse buildFailureResponse(int responseTime, String errorCode,
            String errorMessage, List<String> troubleshooting) {
        return TestConnectionResponse.builder()
                .success(false)
                .status("failure")
                .message("Connection failed")
                .responseTimeMs(responseTime)
                .errorCode(errorCode)
                .errorMessage(errorMessage)
                .troubleshooting(troubleshooting)
                .build();
    }

    private TestConnectionResponse buildNotSupportedResponse(int responseTime, String provider) {
        return TestConnectionResponse.builder()
                .success(false)
                .status("not_supported")
                .message("Connection test not supported for " + provider)
                .responseTimeMs(responseTime)
                .errorCode("NOT_SUPPORTED")
                .errorMessage("Connection testing for " + provider + " is not yet implemented")
                .troubleshooting(Arrays.asList(
                        "This provider is not supported yet",
                        "Contact support for assistance",
                        "Supported providers: PostgreSQL, MySQL, MongoDB, Redis, Oracle, MSSQL, MariaDB"))
                .build();
    }
}