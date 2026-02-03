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
    private List<String> primaryKeys;
    private List<String> additionalBindVariables;
    private String hashTag;
    private String keyIdentifier;
    private String tableName;
    private boolean removeQueryParam = false;

    /**
     Cassandra Writer config Properties
     */
    private String cassandraKeyspace;
    private String cassandraQuery;
    private String beforeWriteQuery; //For executing a single query before writes sent
    private boolean isIdempotent = true;
    private String cassandraClusterName;
    private boolean isCassandraCluster;
    private boolean counterTable = false;
    private int retryCount = 3;
    private int retryAttempts = 3;
    private int retryIntervalInMillis = 3000;
    private String consistencyLevel = "LOCAL_ONE";
    private String truncateTable;
    private boolean removeCurrentDayData = false;
    private String tableSuffix = "";
    private boolean enableSSL = false;
    private List<WriterConfig> restCalls;


    /**
     Redis Writer config Properties
     */

//    private String redisHostName;
//    private int redisPort;
//    private String redisPassword;
//    private int redisDatabase;
//    private List<RedisOperation> redisOperations;
    private boolean isRedisCluster;
    private String redisDelimiter = ":";
    private long redisWriteTimeout = 60000;
    private String redisMapper;
    private int redisPartitionSize = 500;

    /**
     Flat File config Properties
     */
    private String delimiter = ",";
    private String rootTag; /**Tag around all data */
    private String childTag; /** Tag around each row */
    private List<String> fieldLengths;
    private String fileLocation = "local";

    /**
     Custom fields
     */
    private String dataDate;
    private String resourceName;

    /**
     SFTP config Properties
     */
    private String sftpRemoteDir;
    private String sftpMoveDir;
    private String sftpFileName;
    private String sftpCsvHeaderRow;
    private String sftpCsvFooterRow;
    private String sftpFileNameDateFormat;
    private int sftpFileNameDaysToSubtract;
    //delete old files in archive folder aka path of archive = config.getAfterJobConfig().getWriterConfig().getSftpMoveDir()
    private boolean deleteOldFiles = false;
    //anything over 30 days ie 720 hours will be removed
    private long fileExpiryInHours = 720;
    private String zoneId = "UTC";
    private char lineSeparator = '\n';
    private boolean writeFooterOnIncompleteJob = false;

    /**
     EndPoint config Properties
     */
    private String endPointUrl;
    private String requestMethod;
    private Map<String,Object> headers;
    private List<String> body;
    private long throttleTime;
    private int endPointConnectionTimeout = 120000;
    private int endPointSocketTimeout = 20000;
    private int concurrencyMaxTotal = 200;
    private int concurrencyMaxPerRoute = 100;
    private boolean allowNulls = false;
    /**
     Elasticsearch config properties
     */

    private String elasticsearchIndex;
    private List<String> elasticsearchIdColumns;
    private String elasticsearchIdDelimiter = ":";
    private String insertScript = "";
    private String elasticsearchMapper = "";
    private String elasticsearchListName = "";
    private boolean monthSuffix = true;

    /**
     JMS Writer config properties
     */
    private String jmsServiceName = "";
    private String queueName;
    private String topicName;
    private String jmsMapper = null;
    private String JmsPayloadJsonStringWriterMapper = null;
    private Map<String, Boolean> jmsMessageProperties = new HashMap<>();
    private Long timeToLive;
    private List<String> groupBy;
    private String groupName;
    private Long incrementalTTL;
    private boolean isJmsRespectTTL = true;
    private boolean filterEmptyMessages = true;
    private int threshold = 10; //default set limit to 10
    /**
     JDBC config Properties
     */
    private Map<String, Object> jdbcParameterValues;

    /**
     * DME2 Writer properties
     */
    private Map<String, String> requestHeaders = new HashMap<>();
    private Map<String, String> urlParams = new HashMap<>();
    private Map<String, String> queryParams = new HashMap<>();
    private String serviceName;
    private String contentType;
    private String messageId;
    private Map<String, String> messageIdParams = new HashMap<>();
    private int maxAttempts = 1;
    private boolean useRowAsBody;
    private String messageTemplate;
    private String messagePrefix;
    private String messageSuffix;
    private String messagePlaceholder;
    private boolean useSystemLineSeparator = false;
    private String requestBody;
    private String jdbcQuery;
    private String url;
    private String targetMethod;
    private boolean failJobOnErrorResponse = true;
    private List<String> allowedErrorCodes = new ArrayList<>();
    private List<WriterConfig> dme2Calls;
    private boolean writeFlagToCassandra = false;
    private boolean writeErrorFlagToCassandra = false;
    private boolean writeResponsesToCassandra = false;
    private boolean validateResponse = false;
    private boolean preventAdditionalCallsOnError = false;
    private String validateJsonPath;
    private String expectedResponseValue;
    private boolean base64 = false;
    private String base64Prefix;
    private String base64Suffix;

    private boolean writeFlagToPostgres = false;
    private boolean writeErrorFlagToPostgres = false;
    private boolean writeResponsesToPostgres = false;
    private boolean writeConfigNameAndId = false;
    private List<String> finalAdditionalBindVariables;
    private String postgresQuery;
    private boolean writeFlagToReferenceDb = false;
    private String referenceQuery;

    /*Rest Template writer*/
    private String lgwUrl;
    /**
     Azure Event Hub properties
     These aren't used for now, only the proxyUrl
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
     AWS S3 config properties
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
    private boolean ordered = false;
    private boolean upsert = true;
    private boolean mapToDocument = false;
    private boolean removePrimaryKeys = true; //allow you to keep primary keys within document
    private boolean idObject = false; //allows a single field _id to be a single-property object
    private MongoOperation mongoOperation = MongoOperation.SET;
    public enum MongoOperation {
        SET, PUSH, UPDATE_CHILD_OBJECT
    }
    private String arrayPath;
    private String objectPath;
    private String parseDatePattern;
    private boolean processDocument = false;
    //  private UuidRepresentation uuidRepresentation = UuidRepresentation.JAVA_LEGACY;
    // Timeout
    private int connectionTimeout;
    private int readTimeout;
    private String baseUrl;
    private int numThreads;

    private String salesforceObjectName;

    // Rest Templates
    private boolean useTlsV3RestTemplate = false;
    private String azureBlobUploadConnectionName = null;
    private String sourceAzureBlobUploadConnectionName = null;
    private String generatedReportFileName =  null;
    private String generatedReportFileFormat = "xlsx";
    private boolean uploadReportToBlobStorage = false;
    private String azureContainerName;
    private String sourceAzureBlobFileName;
    private String sourceAzureContainerName;
    private String sourceAzureBlobFileFormat = "csv";
    private String azureBlobPrefix;
    private String fileFormat = "xlsx";
    private String soqlQuery;
    private String soqlEntity;
    private List<String> additionalFields;
    private List<String> removeFields;
    private int bulkSobject = 50;
    private String externalId;

    @JsonProperty(value="isCassandraCluster")
    public boolean isCassandraCluster() {
        return this.isCassandraCluster;
    }

    @JsonProperty(value="isRedisCluster")
    public boolean isRedisCluster() {
        return this.isRedisCluster;
    }

    @JsonProperty(value="counterTable")
    public boolean isCounterTable() {
        return this.counterTable;
    }

    @JsonProperty(value="isJmsRespectTTL")
    public boolean isJmsRespectTTL() {
        return this.isJmsRespectTTL;
    }

    @JsonProperty(value="enableSSL")
    public boolean isEnableSSL() {
        return this.enableSSL;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("WriterConfig(");
        if (writer != null) sb.append("writer=").append(writer).append(", ");
        if (writerType != null) sb.append("writerType=").append(writerType).append(", ");
        if (writerBuilder != null) sb.append("writerBuilder=").append(writerBuilder).append(", ");
        if (tableName != null) sb.append("tableName=").append(tableName).append(", ");
        if (connectionName != null) sb.append("connectionName=").append(connectionName).append(", ");
        if (sftpRemoteDir != null) sb.append("sftpRemoteDir=").append(sftpRemoteDir).append(", ");
        if (sftpFileName != null) sb.append("sftpFileName=").append(sftpFileName).append(", ");
        if (sb.length() > 13) sb.setLength(sb.length() - 2);
        sb.append(")");
        return sb.toString();
    }

}

