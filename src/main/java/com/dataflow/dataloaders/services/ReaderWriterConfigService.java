package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.entity.ConfigField;
import com.dataflow.dataloaders.entity.ReaderWriterConfigType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class ReaderWriterConfigService {

    // Predefined reader configurations
    public static final Map<String, ReaderWriterConfigType> READER_CONFIGS = new LinkedHashMap<>();
    
    // Predefined writer configurations  
    public static final Map<String, ReaderWriterConfigType> WRITER_CONFIGS = new LinkedHashMap<>();

    static {
        // JDBC Reader Configuration
        READER_CONFIGS.put("MySQL", ReaderWriterConfigType.builder()
                .id("RC-000001")
                .typeName("MySQL")
                .category("READER")
                .description("MySQL database reader configuration")
                .baseConfig(Map.of(
                        "readerType", "db",
                        "reader", "jdbc", 
                        "readerBuilder", "jdbcCursor",
                        "readerName", "jdbcCursor"
                ))
                .configFields(Arrays.asList(
                        ConfigField.builder().name("query").label("SQL Query")
                                .type("textarea").required(true).order(1)
                                .placeholder("SELECT * FROM table_name").build(),
                        ConfigField.builder().name("fetchSize").label("Fetch Size")
                                .type("number").required(false).defaultValue(1000).min(100).max(10000).order(2).build(),
                        ConfigField.builder().name("queryTimeout").label("Query Timeout (seconds)")
                                .type("number").required(false).defaultValue(300).min(30).max(3600).order(3).build()
                ))
                .isActive(true)
                .build());

        READER_CONFIGS.put("PostgreSQL", ReaderWriterConfigType.builder()
                .id("RC-000002")
                .typeName("PostgreSQL")
                .category("READER")
                .description("PostgreSQL database reader configuration")
                .baseConfig(Map.of(
                        "readerType", "db",
                        "reader", "jdbc",
                        "readerBuilder", "jdbcCursor", 
                        "readerName", "jdbcCursor"
                ))
                .configFields(Arrays.asList(
                        ConfigField.builder().name("query").label("SQL Query")
                                .type("textarea").required(true).order(1).build(),
                        ConfigField.builder().name("fetchSize").label("Fetch Size")
                                .type("number").required(false).defaultValue(1000).order(2).build(),
                        ConfigField.builder().name("cursorType").label("Cursor Type")
                                .type("select").required(false).defaultValue("FORWARD_ONLY").order(3)
                                .options("[\"FORWARD_ONLY\", \"SCROLL_INSENSITIVE\", \"SCROLL_SENSITIVE\"]").build()
                ))
                .isActive(true)
                .build());

        READER_CONFIGS.put("SFTP", ReaderWriterConfigType.builder()
                .id("RC-000003")
                .typeName("SFTP")
                .category("READER")
                .description("SFTP file reader configuration")
                .baseConfig(Map.of(
                        "readerType", "file",
                        "reader", "csv",
                        "readerBuilder", "sftp",
                        "readerName", "sftp"
                ))
                .configFields(Arrays.asList(
                        ConfigField.builder().name("filePath").label("File Path")
                                .type("text").required(true).order(1)
                                .placeholder("/path/to/file.csv").build(),
                        ConfigField.builder().name("delimiter").label("Delimiter")
                                .type("text").required(false).defaultValue(",").order(2).build(),
                        ConfigField.builder().name("hasHeader").label("Has Header Row")
                                .type("boolean").required(false).defaultValue(true).order(3).build(),
                        ConfigField.builder().name("encoding").label("File Encoding")
                                .type("select").required(false).defaultValue("UTF-8").order(4)
                                .options("[\"UTF-8\", \"ISO-8859-1\", \"ASCII\"]").build()
                ))
                .isActive(true)
                .build());

        // CSV Writer Configuration (SFTP)
        WRITER_CONFIGS.put("SFTP", ReaderWriterConfigType.builder()
                .id("WC-000001")
                .typeName("SFTP")
                .category("WRITER")
                .description("SFTP CSV file writer configuration")
                .baseConfig(Map.of(
                        "writer", "csv",
                        "fileLocation", "sftp",
                        "writerBuilder", "sftp",
                        "writerName", "sftp"
                ))
                .configFields(Arrays.asList(
                        ConfigField.builder().name("outputPath").label("Output File Path")
                                .type("text").required(true).order(1)
                                .placeholder("/output/path/file.csv").build(),
                        ConfigField.builder().name("delimiter").label("Delimiter")
                                .type("text").required(false).defaultValue(",").order(2).build(),
                        ConfigField.builder().name("includeHeader").label("Include Header Row")
                                .type("boolean").required(false).defaultValue(true).order(3).build(),
                        ConfigField.builder().name("overwriteMode").label("Overwrite Mode")
                                .type("select").required(false).defaultValue("OVERWRITE").order(4)
                                .options("[\"OVERWRITE\", \"APPEND\", \"ERROR_IF_EXISTS\"]").build()
                ))
                .isActive(true)
                .build());

        WRITER_CONFIGS.put("MongoDB", ReaderWriterConfigType.builder()
                .id("WC-000002")
                .typeName("MongoDB")
                .category("WRITER")
                .description("MongoDB document writer configuration")
                .baseConfig(Map.of(
                        "writer", "mongodb",
                        "fileLocation", "database",
                        "writerBuilder", "mongodb",
                        "writerName", "mongodb"
                ))
                .configFields(Arrays.asList(
                        ConfigField.builder().name("collection").label("Collection Name")
                                .type("text").required(true).order(1).build(),
                        ConfigField.builder().name("writeMode").label("Write Mode")
                                .type("select").required(false).defaultValue("INSERT").order(2)
                                .options("[\"INSERT\", \"UPSERT\", \"REPLACE\"]").build(),
                        ConfigField.builder().name("batchSize").label("Batch Size")
                                .type("number").required(false).defaultValue(1000).min(100).max(10000).order(3).build(),
                        ConfigField.builder().name("upsertKey").label("Upsert Key Field")
                                .type("text").required(false).order(4)
                                .helpText("Field to use for upsert operations").build()
                ))
                .isActive(true)
                .build());

        WRITER_CONFIGS.put("MySQL", ReaderWriterConfigType.builder()
                .id("WC-000003")
                .typeName("MySQL")
                .category("WRITER")
                .description("MySQL database writer configuration")
                .baseConfig(Map.of(
                        "writer", "jdbc",
                        "fileLocation", "database",
                        "writerBuilder", "jdbc",
                        "writerName", "jdbc"
                ))
                .configFields(Arrays.asList(
                        ConfigField.builder().name("tableName").label("Table Name")
                                .type("text").required(true).order(1).build(),
                        ConfigField.builder().name("writeMode").label("Write Mode")
                                .type("select").required(false).defaultValue("INSERT").order(2)
                                .options("[\"INSERT\", \"UPDATE\", \"UPSERT\", \"TRUNCATE_INSERT\"]").build(),
                        ConfigField.builder().name("batchSize").label("Batch Size")
                                .type("number").required(false).defaultValue(1000).order(3).build(),
                        ConfigField.builder().name("preSQL").label("Pre-execution SQL")
                                .type("textarea").required(false).order(4)
                                .placeholder("DELETE FROM temp_table;").build(),
                        ConfigField.builder().name("postSQL").label("Post-execution SQL")
                                .type("textarea").required(false).order(5)
                                .placeholder("UPDATE stats SET last_updated = NOW();").build()
                ))
                .isActive(true)
                .build());
    }

    public ReaderWriterConfigType getReaderConfig(String resourceType) {
        return READER_CONFIGS.get(resourceType);
    }

    public ReaderWriterConfigType getWriterConfig(String resourceType) {
        return WRITER_CONFIGS.get(resourceType);
    }

    public List<ReaderWriterConfigType> getAllReaderConfigs() {
        return new ArrayList<>(READER_CONFIGS.values());
    }

    public List<ReaderWriterConfigType> getAllWriterConfigs() {
        return new ArrayList<>(WRITER_CONFIGS.values());
    }

    public Map<String, Object> buildReaderConfig(String resourceType, Map<String, Object> userConfig) {
        ReaderWriterConfigType readerType = READER_CONFIGS.get(resourceType);
        if (readerType == null) {
            return userConfig != null ? userConfig : new HashMap<>();
        }

        Map<String, Object> finalConfig = new HashMap<>(readerType.getBaseConfig());
        if (userConfig != null) {
            finalConfig.putAll(userConfig);
        }
        return finalConfig;
    }

    public Map<String, Object> buildWriterConfig(String resourceType, Map<String, Object> userConfig) {
        ReaderWriterConfigType writerType = WRITER_CONFIGS.get(resourceType);
        if (writerType == null) {
            return userConfig != null ? userConfig : new HashMap<>();
        }

        Map<String, Object> finalConfig = new HashMap<>(writerType.getBaseConfig());
        if (userConfig != null) {
            finalConfig.putAll(userConfig);
        }
        return finalConfig;
    }
}