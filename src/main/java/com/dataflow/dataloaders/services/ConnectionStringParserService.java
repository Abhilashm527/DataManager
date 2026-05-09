package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dto.ConnectionStringParseResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class ConnectionStringParserService {

    private static final String MASKED = "****";

    /**
     * Parses a connection string and returns the mapped fields with the password masked.
     * Supports: PostgreSQL, MySQL, MariaDB, MongoDB, Redis, Oracle, MSSQL
     */
    public ConnectionStringParseResponse parse(String connectionString) {
        if (connectionString == null || connectionString.isBlank()) {
            return invalid("Connection string must not be empty");
        }

        String trimmed = connectionString.trim();

        try {
            if (trimmed.startsWith("jdbc:postgresql://") || trimmed.startsWith("jdbc:postgres://")) {
                return parseJdbcUrl(trimmed, "postgresql");
            } else if (trimmed.startsWith("postgresql://") || trimmed.startsWith("postgres://")) {
                return parseUriStyle(trimmed, "postgresql", 5432);
            } else if (trimmed.startsWith("jdbc:mysql://")) {
                return parseJdbcUrl(trimmed, "mysql");
            } else if (trimmed.startsWith("mysql://")) {
                return parseUriStyle(trimmed, "mysql", 3306);
            } else if (trimmed.startsWith("jdbc:mariadb://")) {
                return parseJdbcUrl(trimmed, "mariadb");
            } else if (trimmed.startsWith("mariadb://")) {
                return parseUriStyle(trimmed, "mariadb", 3306);
            } else if (trimmed.startsWith("mongodb+srv://")) {
                return parseMongoSrv(trimmed);
            } else if (trimmed.startsWith("mongodb://")) {
                return parseMongoDB(trimmed);
            } else if (trimmed.startsWith("rediss://")) {
                return parseRedis(trimmed, true);
            } else if (trimmed.startsWith("redis://")) {
                return parseRedis(trimmed, false);
            } else if (trimmed.startsWith("jdbc:oracle:thin:")) {
                return parseOracle(trimmed);
            } else if (trimmed.startsWith("jdbc:sqlserver://")) {
                return parseMssql(trimmed);
            } else {
                return invalid("Unrecognized connection string format. Supported: PostgreSQL, MySQL, MariaDB, MongoDB, Redis, Oracle, MSSQL");
            }
        } catch (Exception e) {
            log.warn("Failed to parse connection string: {}", e.getMessage());
            return invalid("Failed to parse connection string: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // URI-style: provider://user:pass@host:port/database?params
    // -------------------------------------------------------------------------

    private ConnectionStringParseResponse parseUriStyle(String cs, String providerKey, int defaultPort) throws Exception {
        // Normalize to a parseable URI scheme
        String normalized = cs
                .replaceFirst("^postgresql://", "pg://")
                .replaceFirst("^postgres://", "pg://")
                .replaceFirst("^mysql://", "mysql-scheme://")
                .replaceFirst("^mariadb://", "mariadb-scheme://");

        URI uri = new URI(normalized);

        String host = uri.getHost();
        int port = uri.getPort() > 0 ? uri.getPort() : defaultPort;
        String database = extractPath(uri.getPath());
        Map<String, String> params = parseQueryString(uri.getRawQuery());

        String username = null;
        String passwordRaw = null;

        if (uri.getUserInfo() != null) {
            String[] userInfo = uri.getUserInfo().split(":", 2);
            username = decode(userInfo[0]);
            if (userInfo.length == 2) {
                passwordRaw = decode(userInfo[1]);
            }
        }

        // Fallback to query params
        if (username == null) username = params.remove("user");
        if (username == null) username = params.remove("username");
        if (passwordRaw == null) passwordRaw = params.remove("password");

        Boolean ssl = parseSslParam(params);

        return ConnectionStringParseResponse.builder()
                .valid(true)
                .providerKey(providerKey)
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(passwordRaw != null ? MASKED : null)
                .useSsl(ssl)
                .additionalParams(params.isEmpty() ? null : params)
                .build();
    }

    // -------------------------------------------------------------------------
    // JDBC URL: jdbc:provider://host:port/database?params
    // -------------------------------------------------------------------------

    private ConnectionStringParseResponse parseJdbcUrl(String cs, String providerKey) throws Exception {
        // Strip the jdbc: prefix to get a parseable URI
        String withoutJdbc = cs.replaceFirst("^jdbc:", "");
        URI uri = new URI(withoutJdbc);

        String host = uri.getHost();
        int defaultPort = defaultPortFor(providerKey);
        int port = uri.getPort() > 0 ? uri.getPort() : defaultPort;
        String database = extractPath(uri.getPath());
        Map<String, String> params = parseQueryString(uri.getRawQuery());

        String username = params.remove("user");
        if (username == null) username = params.remove("username");
        String passwordRaw = params.remove("password");

        Boolean ssl = parseSslParam(params);

        return ConnectionStringParseResponse.builder()
                .valid(true)
                .providerKey(providerKey)
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(passwordRaw != null ? MASKED : null)
                .useSsl(ssl)
                .additionalParams(params.isEmpty() ? null : params)
                .build();
    }

    // -------------------------------------------------------------------------
    // MongoDB: mongodb://[user:pass@]host[:port][/database][?options]
    // -------------------------------------------------------------------------

    private ConnectionStringParseResponse parseMongoDB(String cs) throws Exception {
        URI uri = new URI(cs);

        String host = uri.getHost();
        int port = uri.getPort() > 0 ? uri.getPort() : 27017;
        String database = extractPath(uri.getPath());
        Map<String, String> params = parseQueryString(uri.getRawQuery());

        String username = null;
        String passwordRaw = null;

        if (uri.getUserInfo() != null) {
            String[] userInfo = uri.getUserInfo().split(":", 2);
            username = decode(userInfo[0]);
            if (userInfo.length == 2) {
                passwordRaw = decode(userInfo[1]);
            }
        }

        Boolean ssl = parseSslParam(params);

        return ConnectionStringParseResponse.builder()
                .valid(true)
                .providerKey("mongodb")
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(passwordRaw != null ? MASKED : null)
                .useSsl(ssl)
                .additionalParams(params.isEmpty() ? null : params)
                .build();
    }

    // -------------------------------------------------------------------------
    // MongoDB SRV: mongodb+srv://user:pass@host/database?options
    // -------------------------------------------------------------------------

    private ConnectionStringParseResponse parseMongoSrv(String cs) throws Exception {
        // Replace mongodb+srv:// with a dummy scheme for URI parsing
        URI uri = new URI(cs.replaceFirst("^mongodb\\+srv://", "mongodbsrv://"));

        String host = uri.getHost();
        String database = extractPath(uri.getPath());
        Map<String, String> params = parseQueryString(uri.getRawQuery());

        String username = null;
        String passwordRaw = null;

        if (uri.getUserInfo() != null) {
            String[] userInfo = uri.getUserInfo().split(":", 2);
            username = decode(userInfo[0]);
            if (userInfo.length == 2) {
                passwordRaw = decode(userInfo[1]);
            }
        }

        Boolean ssl = parseSslParam(params);

        Map<String, String> extra = new HashMap<>(params);
        extra.put("srvMode", "true");

        return ConnectionStringParseResponse.builder()
                .valid(true)
                .providerKey("mongodb")
                .host(host)
                .database(database)
                .username(username)
                .password(passwordRaw != null ? MASKED : null)
                .useSsl(ssl)
                .additionalParams(extra.isEmpty() ? null : extra)
                .build();
    }

    // -------------------------------------------------------------------------
    // Redis: redis[s]://[:password@]host[:port][/db-number]
    // -------------------------------------------------------------------------

    private ConnectionStringParseResponse parseRedis(String cs, boolean isSsl) throws Exception {
        URI uri = new URI(cs);

        String host = uri.getHost();
        int port = uri.getPort() > 0 ? uri.getPort() : 6379;

        String passwordRaw = null;
        String username = null;

        if (uri.getUserInfo() != null) {
            String userInfo = uri.getUserInfo();
            if (userInfo.startsWith(":")) {
                // redis://:password@host format (no username)
                passwordRaw = decode(userInfo.substring(1));
            } else {
                String[] parts = userInfo.split(":", 2);
                username = decode(parts[0]);
                if (parts.length == 2) {
                    passwordRaw = decode(parts[1]);
                }
            }
        }

        String dbNumber = extractPath(uri.getPath());
        Map<String, String> params = parseQueryString(uri.getRawQuery());

        return ConnectionStringParseResponse.builder()
                .valid(true)
                .providerKey("redis")
                .host(host)
                .port(port)
                .database(dbNumber)
                .username(username)
                .password(passwordRaw != null ? MASKED : null)
                .useSsl(isSsl)
                .additionalParams(params.isEmpty() ? null : params)
                .build();
    }

    // -------------------------------------------------------------------------
    // Oracle JDBC: jdbc:oracle:thin:[user/pass@]@host:port:service
    //              jdbc:oracle:thin:@//host:port/service
    // -------------------------------------------------------------------------

    private ConnectionStringParseResponse parseOracle(String cs) {
        // Strip jdbc:oracle:thin:
        String rest = cs.substring("jdbc:oracle:thin:".length());

        String username = null;
        String passwordRaw = null;

        // Optional credentials before the @
        if (!rest.startsWith("@")) {
            int atIdx = rest.lastIndexOf('@');
            if (atIdx > 0) {
                String credsPart = rest.substring(0, atIdx);
                rest = rest.substring(atIdx);
                String[] creds = credsPart.split("/", 2);
                username = creds[0];
                if (creds.length == 2) passwordRaw = creds[1];
            }
        }

        // Remove leading @
        if (rest.startsWith("@")) rest = rest.substring(1);

        String host = null;
        Integer port = null;
        String service = null;

        if (rest.startsWith("//")) {
            // jdbc:oracle:thin:@//host:port/service
            rest = rest.substring(2);
            String[] hostPart = rest.split("/", 2);
            String[] hostPort = hostPart[0].split(":", 2);
            host = hostPort[0];
            if (hostPort.length == 2) port = parsePort(hostPort[1], 1521);
            if (hostPart.length == 2) service = hostPart[1];
        } else {
            // jdbc:oracle:thin:@host:port:service
            String[] parts = rest.split(":", 3);
            if (parts.length >= 1) host = parts[0];
            if (parts.length >= 2) port = parsePort(parts[1], 1521);
            if (parts.length >= 3) service = parts[2];
        }

        return ConnectionStringParseResponse.builder()
                .valid(true)
                .providerKey("oracle")
                .host(host)
                .port(port != null ? port : 1521)
                .database(service)
                .username(username)
                .password(passwordRaw != null ? MASKED : null)
                .build();
    }

    // -------------------------------------------------------------------------
    // MSSQL: jdbc:sqlserver://host:port;databaseName=...;user=...;password=...
    // -------------------------------------------------------------------------

    private ConnectionStringParseResponse parseMssql(String cs) throws Exception {
        // The part after jdbc:sqlserver:// can have host:port;key=value pairs
        String rest = cs.substring("jdbc:sqlserver://".length());

        String host = null;
        Integer port = 1433;

        int semicolonIdx = rest.indexOf(';');
        String hostPart = semicolonIdx > 0 ? rest.substring(0, semicolonIdx) : rest;
        String propsPart = semicolonIdx > 0 ? rest.substring(semicolonIdx + 1) : "";

        String[] hostPort = hostPart.split(":", 2);
        if (hostPort.length >= 1 && !hostPort[0].isBlank()) host = hostPort[0];
        if (hostPort.length == 2) port = parsePort(hostPort[1], 1433);

        Map<String, String> props = parseSemicolonProps(propsPart);

        String database = props.remove("databaseName");
        if (database == null) database = props.remove("database");
        String username = props.remove("user");
        if (username == null) username = props.remove("username");
        String passwordRaw = props.remove("password");

        Boolean ssl = null;
        String encrypt = props.remove("encrypt");
        if (encrypt != null) ssl = "true".equalsIgnoreCase(encrypt);

        return ConnectionStringParseResponse.builder()
                .valid(true)
                .providerKey("mssql")
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(passwordRaw != null ? MASKED : null)
                .useSsl(ssl)
                .additionalParams(props.isEmpty() ? null : props)
                .build();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private ConnectionStringParseResponse invalid(String message) {
        return ConnectionStringParseResponse.builder()
                .valid(false)
                .errorMessage(message)
                .build();
    }

    private String extractPath(String path) {
        if (path == null || path.isBlank() || path.equals("/")) return null;
        return path.startsWith("/") ? path.substring(1) : path;
    }

    private Map<String, String> parseQueryString(String query) {
        Map<String, String> map = new HashMap<>();
        if (query == null || query.isBlank()) return map;
        for (String pair : query.split("&")) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                map.put(decode(kv[0]), decode(kv[1]));
            } else if (kv.length == 1 && !kv[0].isBlank()) {
                map.put(decode(kv[0]), "");
            }
        }
        return map;
    }

    private Map<String, String> parseSemicolonProps(String propsStr) {
        Map<String, String> map = new HashMap<>();
        if (propsStr == null || propsStr.isBlank()) return map;
        for (String prop : propsStr.split(";")) {
            String[] kv = prop.split("=", 2);
            if (kv.length == 2 && !kv[0].isBlank()) {
                map.put(kv[0].trim(), kv[1].trim());
            }
        }
        return map;
    }

    private Boolean parseSslParam(Map<String, String> params) {
        String ssl = params.remove("ssl");
        if (ssl == null) ssl = params.remove("useSSL");
        if (ssl == null) ssl = params.remove("sslmode");
        if (ssl == null) return null;
        return "true".equalsIgnoreCase(ssl) || "require".equalsIgnoreCase(ssl) || "verify-full".equalsIgnoreCase(ssl);
    }

    private int defaultPortFor(String providerKey) {
        switch (providerKey) {
            case "postgresql": return 5432;
            case "mysql":
            case "mariadb": return 3306;
            case "mongodb": return 27017;
            case "redis": return 6379;
            case "oracle": return 1521;
            case "mssql": return 1433;
            default: return -1;
        }
    }

    private int parsePort(String portStr, int defaultPort) {
        try {
            return Integer.parseInt(portStr.trim());
        } catch (NumberFormatException e) {
            return defaultPort;
        }
    }

    private String decode(String value) {
        if (value == null) return null;
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }
}
