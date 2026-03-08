package com.dataflow.dataloaders.jobconfigs;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.stereotype.Component;

import java.util.*;

@Getter
@Setter
@Component
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WriterConfig {

    private String writer;
    private String writerName;
    private String writerBuilder;
    private String writerType;
    private String outputFile;
    private String username;
    private String password;
    private ConnectionConfig connectionConfig;
    private ConnectionConfig restFlagConnectionConfig;
    private String restFlagConnectionName;
    private String connectionName;
    private String connectionId;
    private String schemaName;
    private List<String> primaryKeys;
    private List<String> additionalBindVariables;
    private String hashTag;
    private String keyIdentifier;
    private String tableName;
    private Boolean removeQueryParam ;

    /**
     * Cassandra Writer config Properties
     */
    private String cassandraKeyspace;
    private String cassandraQuery;
    private String beforeWriteQuery; // For executing a single query before writes sent
    private Boolean isIdempotent ;
    private String cassandraClusterName;
    private Boolean isCassandraCluster;
    private Boolean counterTable ;
    private Integer retryCount ;
    private Integer retryAttempts ;
    private Integer retryIntervalInMillis ;
    private String consistencyLevel ;
    private String truncateTable;
    private Boolean removeCurrentDayData ;
    private String tableSuffix ;
    private Boolean enableSSL ;
    private List<WriterConfig> restCalls;

    /**
     * Redis Writer config Properties
     */

    // private String redisHostName;
    // private int redisPort;
    // private String redisPassword;
    // private int redisDatabase;
    // private List<RedisOperation> redisOperations;
    private Boolean isRedisCluster;
    private String redisDelimiter ;
    private Long redisWriteTimeout ;
    private String redisMapper;
    private Integer redisPartitionSize ;

    /**
     * Flat File config Properties
     */
    private String delimiter = ",";
    private String rootTag;
    /** Tag around all data */
    private String childTag;
    /** Tag around each row */
    private List<String> fieldLengths;
    private String fileLocation ;

    /**
     * Custom fields
     */
    private String dataDate;
    private String resourceName;

    /**
     * SFTP config Properties
     */
    private String sftpRemoteDir;
    private String sftpMoveDir;
    private String sftpFileName;
    private String sftpCsvHeaderRow;
    private String sftpCsvFooterRow;
    private String sftpFileNameDateFormat;
    private Integer sftpFileNameDaysToSubtract;
    // delete old files in archive folder aka path of archive =
    // config.getAfterJobConfig().getWriterConfig().getSftpMoveDir()
    private Boolean deleteOldFiles ;
    // anything over 30 days ie 720 hours will be removed
    private Long fileExpiryInHours ;
    private String zoneId ;
    private Character lineSeparator ;
    private Boolean writeFooterOnIncompleteJob ;

    /**
     * EndPoint config Properties
     */
    private String endPointUrl;
    private String requestMethod;
    private Map<String, Object> headers;
    private List<String> body;
    private Long throttleTime;
    private Integer endPointConnectionTimeout ;
    private Integer endPointSocketTimeout ;
    private Integer concurrencyMaxTotal ;
    private Integer concurrencyMaxPerRoute ;
    private Boolean allowNulls ;
    /**
     * Elasticsearch config properties
     */

    private String elasticsearchIndex;
    private List<String> elasticsearchIdColumns;
    private String elasticsearchIdDelimiter ;
    private String insertScript ;
    private String elasticsearchMapper ;
    private String elasticsearchListName ;
    private Boolean monthSuffix ;

    /**
     * JMS Writer config properties
     */
    private String jmsServiceName ;
    private String queueName;
    private String topicName;
    private String jmsMapper ;
    private String JmsPayloadJsonStringWriterMapper ;
    private Map<String, Boolean> jmsMessageProperties ;
    private Long timeToLive;
    private List<String> groupBy;
    private String groupName;
    private Long incrementalTTL;
    private Boolean isJmsRespectTTL ;
    private Boolean filterEmptyMessages ;
    private Integer threshold ; // default set limit to 10
    /**
     * JDBC config Properties
     */
    private Map<String, Object> jdbcParameterValues;

    /**
     * DME2 Writer properties
     */
    private Map<String, String> requestHeaders ;
    private Map<String, String> urlParams ;
    private Map<String, String> queryParams ;
    private String serviceName;
    private String contentType;
    private String messageId;
    private Map<String, String> messageIdParams ;
    private Integer maxAttempts ;
    private Boolean useRowAsBody;
    private String messageTemplate;
    private String messagePrefix;
    private String messageSuffix;
    private String messagePlaceholder;
    private Boolean useSystemLineSeparator;
    private String requestBody;
    private String jdbcQuery;
    private String url;
    private String targetMethod;
    private Boolean failJobOnErrorResponse ;
    private List<String> allowedErrorCodes = new ArrayList<>();
    private List<WriterConfig> dme2Calls;
    private Boolean writeFlagToCassandra ;
    private Boolean writeErrorFlagToCassandra ;
    private Boolean writeResponsesToCassandra ;
    private Boolean validateResponse ;
    private Boolean preventAdditionalCallsOnError ;
    private String validateJsonPath;
    private String expectedResponseValue;
    private Boolean base64 ;
    private String base64Prefix;
    private String base64Suffix;

    private Boolean writeFlagToPostgres ;
    private Boolean writeErrorFlagToPostgres ;
    private Boolean writeResponsesToPostgres ;
    private Boolean writeConfigNameAndId ;
    private List<String> finalAdditionalBindVariables;
    private String postgresQuery;
    private Boolean writeFlagToReferenceDb ;
    private String referenceQuery;

    /* Rest Template writer */
    private String lgwUrl;
    /**
     * Azure Event Hub properties
     * These aren't used for now, only the proxyUrl
     */
    private String sendConnectionString;
    private String listenConnectionString;
    private String keyVaultTenantId;
    private String keyVaultUrl;
    private String keyVaultClientId;
    private String keyVaultClientKey;
    private String proxyUrl;
    private Integer proxyPort;
    private List<String> ignoreFields;

    /**
     * AWS S3 config properties
     */
    private Date transactionCutoff;

    /**
     * EventHub config properties
     */
    private String ehEnv;

    /**
     * MongoDB properties
     */
    private String mongoDatabase;
    private String mongoCollection;
    private Boolean ordered ;
    private Boolean upsert ;
    private Boolean mapToDocument ;
    private Boolean removePrimaryKeys ; // allow you to keep primary keys within document
    private Boolean idObject ; // allows a single field _id to be a single-property object
    private MongoOperation mongoOperation ;

    public enum MongoOperation {
        SET, PUSH, UPDATE_CHILD_OBJECT
    }

    private String arrayPath;
    private String objectPath;
    private String parseDatePattern;
    private Boolean processDocument ;
    // private UuidRepresentation uuidRepresentation =
    // UuidRepresentation.JAVA_LEGACY;
    // Timeout
    private Integer connectionTimeout;
    private Integer readTimeout;
    private String baseUrl;
    private Integer numThreads;

    private String salesforceObjectName;

    // Rest Templates
    private Boolean useTlsV3RestTemplate ;
    private String azureBlobUploadConnectionName ;
    private String sourceAzureBlobUploadConnectionName ;
    private String generatedReportFileName ;
    private String generatedReportFileFormat ;
    private Boolean uploadReportToBlobStorage ;
    private String azureContainerName;
    private String sourceAzureBlobFileName;
    private String sourceAzureContainerName;
    private String sourceAzureBlobFileFormat ;
    private String azureBlobPrefix;
    private String fileFormat ;
    private String soqlQuery;
    private String soqlEntity;
    private List<String> additionalFields;
    private List<String> removeFields;
    private Integer bulkSobject ;
    private String externalId;

    @JsonProperty(value = "isCassandraCluster")
    public boolean isCassandraCluster() {
        return this.isCassandraCluster;
    }

    @JsonProperty(value = "isRedisCluster")
    public boolean isRedisCluster() {
        return this.isRedisCluster;
    }

    @JsonProperty(value = "counterTable")
    public boolean isCounterTable() {
        return this.counterTable;
    }

    @JsonProperty(value = "isJmsRespectTTL")
    public boolean isJmsRespectTTL() {
        return this.isJmsRespectTTL;
    }

    @JsonProperty(value = "enableSSL")
    public boolean isEnableSSL() {
        return this.enableSSL;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("WriterConfig(");
        if (writer != null)
            sb.append("writer=").append(writer).append(", ");
        if (writerType != null)
            sb.append("writerType=").append(writerType).append(", ");
        if (writerBuilder != null)
            sb.append("writerBuilder=").append(writerBuilder).append(", ");
        if (tableName != null)
            sb.append("tableName=").append(tableName).append(", ");
        if (connectionName != null)
            sb.append("connectionName=").append(connectionName).append(", ");
        if (sftpRemoteDir != null)
            sb.append("sftpRemoteDir=").append(sftpRemoteDir).append(", ");
        if (sftpFileName != null)
            sb.append("sftpFileName=").append(sftpFileName).append(", ");
        if (sb.length() > 13)
            sb.setLength(sb.length() - 2);
        sb.append(")");
        return sb.toString();
    }

}
