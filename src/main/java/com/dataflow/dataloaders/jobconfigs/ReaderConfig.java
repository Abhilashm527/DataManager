package com.dataflow.dataloaders.jobconfigs;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Component
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReaderConfig {

    private ConnectionConfig connectionConfig;
    private String connectionName;
    private List<String> primaryKeys;
    private String partitioner;
    private String tableName;

    private String reader;
    private String readerType;
    private String inputFile;
    private String readerName;
    private String readerBuilder;

    private String sftpRemoteDir;
    private String sftpFileName;
    private String sftpFileType;
    private String sftpFileNameSubstring;

    /**
     * Camel Routing
     */
    private Boolean camelRoute;
    private String pgpEncryptedInput;
    private String pgpEncryptedInputOptions;
    private String pgpDecryptedOutput;
    private String pgpDecryptedOutputOptions;
    private String pgpEncryptedArchive;
    private String pgpEncryptedArchiveOptions;
    private Long camelTimeout;
    private String pgpUserId;
    private String pgpPassword;
    private String pgpSecretKey;

    // SAP Connection Configuration
    private String sapOdataServiceUrl;
    private String sapEntitySetName;
    private String sapOdataFilter;
    private String sapOdataOrderBy;
    private String sapMapper;

    /**
     * Cassandra Reader config Properties
     */
    private String cassandraKeyspace;
    private String cassandraClusterName;
    private Boolean isCassandraCluster;
    private Boolean condenseRows;
    private String condenseRowField;
    private String cassandraQuery;
    private Boolean isIdempotent = true;
    private Boolean isBulk;
    private Boolean allowFiltering;
    private Integer retryAttempts = 5;
    private String consistencyLevel = "LOCAL_ONE";
    // private List<AdditionalQuery> additionalQueries;
    private SPLIT_ROW_OPTION splitRow;
    private LinkedHashMap<String, Object> splitRowParams;
    private Boolean enableSSL = false;
    private Integer retryCount = 3;

    public enum SPLIT_ROW_OPTION {
        DUPLICATE_ROW_PER_JSON_ARRAY_ENTRY // used to extract multiple rows based on an array which is stored within a
                                           // JSON field
    }

    private DateBindType bindDate;
    private DateBindType bindDate2;
    private DateBindType bindDate3;
    private List<DateBindType> bindDates;
    private DateBindType bindTimestamp;
    private List<String> bindDateArgument;
    private List<String> bindFieldNames;
    private MultiQueryBindType multiQueryBind;
    private Integer numberOfDays;
    private Integer numberOfDaysToAdd;
    private Boolean monthZeroIndexed = false;

    private TokenBindType tokenBindType;
    private Integer tokenIntervalCount = 0;
    private Integer tokenRangeIndex;

    public enum TokenBindType {
        YESTERDAY, TODAY, MANUAL
    }

    public enum DateBindType {
        TODAY, TODAY_LOCALDATE, CURRENT_DAY_INT, YESTERDAY, YESTERDAY_LOCALDATE, YESTERDAY_STRING, YESTERDAY_INT,
        TOMORROW, TOMORROW_LOCALDATE, VARIABLE_DATE,
        SPLIT_DATE, SPLIT_DATE_HOUR, CURRENT_HOUR_INT, CURRENT_HOUR_TIMESTAMP, LAST_HOUR_INT, LAST_HOUR_TIMESTAMP,
        LAST_WEEK, CURRENT_MONTH_INT, SPLIT_DATE_TODAY,
        CURRENT_MONTH_START, LAST_MONTH_INT, LAST_MONTH_START, CURRENT_YEAR_INT, LAST_YEAR_INT, LAST_N_DAYS,
        DAY_BEFORE_YESTERDAY_LOCALDATE, SPLIT_DATE_PREVIOUS_HOUR, NEXT_N_DAYS
    }

    public enum MultiQueryBindType {
        LAST_N_DAYS_YEAR_MONTH_DAY, INTERVAL_YEAR_MONTH_DAY
    }

    /*
     * For Cassandra Reader when setting up DateBindTypes with a zoneId aka
     * timezone, list of all timezone strings below
     * https://mkyong.com/java8/java-display-all-zoneid-and-its-utc-offset/
     */

    private String zoneId = "UTC";

    private String selectClause;
    private String fromClause;
    private String whereClause = "";
    private String suffixClause = "";
    private String groupClause;

    private String foreignKeyQuery;
    private String foreignKeyName;
    private String foreignKeyType;
    private int foreignKeyChunkSize;
    private String cassandraMapper;

    /**
     * Redis Reader config Properties
     */

    // private List<RedisOperation> redisOperations;
    private Boolean isRedisCluster;
    private String redisDelimiter = ":";
    private Boolean isRedisConsumerStream = false;
    private long checkPendingMessagesThreshold = 5000;
    private String redisMapper = "";

    /**
     * JDBC Reader config Properties
     */
    private Map<String, String> jdbcSortKeys;
    private LinkedHashMap<String, Object> jdbcParameterValues;
    private String jdbcStateName;

    /**
     * Elasticsearch config properties
     */
    private String elasticsearchQuery;
    private Long scrollTimeout;
    private String elasticsearchIndex;

    /**
     * File Reader config Properties
     */
    private String delimiter = ",";
    private Integer linesToSkip = 0;
    private String commentIndicator = "//";
    private String trueValue = "true";
    private String dateFormat;
    private String rootElement;
    private List<String> fieldLengths;
    private String fileLocation = "local";
    private List<Integer> includedFields;
    private Integer malformedLinesToSkip = 0;
    private Integer maxItemCount = Integer.MAX_VALUE;
    private Boolean strict = true;
    private String expectedHeaderRow;
    // if there is a double quote within the txt file, will throw exception
    // because LinkTokenizer will treat entire row as one column since there isn't
    // an ending quote,
    // use setQuoteCharacter to '@' or any other character not used within that file
    // to prevent exception
    // example: D|PIXI 4 6" 3G Android|12345, will be split into two instead of
    // three columns D, PIXI 4 6" 3G Android|12345
    private String setQuoteCharacter;

    /**
     * JMS Reader config Properties
     */
    private String queueName;
    private String topicName;
    private String jmsMapper = "null";
    private Boolean isJmsRespectTTL = true;
    private Boolean isMsgQueueEmpty = false; // should be added in config as true to stop the reader config if msg is
                                             // empty/null

    /**
     * DME2 Reader config properties
     */
    private String serviceName;
    private String serviceUrl;
    private Long timeout;
    private String targetMethod;
    private String targetVersion;
    private String url;
    private String lgwUrl;
    private Map<String, String> uriQueryParam;
    private String arrayJsonPath;
    private Map<String, String> headers = new HashMap<>();
    private Integer maxAttempts = 1;

    /**
     * DME2 CSV Reader config properties
     */
    private List<Map<String, String>> urlParameters;
    private Map<String, DateBindType> dateUrlParameters = new HashMap<>();

    /**
     * Azure blob config
     */
    private String azureBlobContainer; // The container name (e.g., "my-container")
    private String azureBlobFileName;
    /**
     * 
     * ExcelFileReader config properties
     */
    private Integer sheetNumber = 0;
    private String sheetName = null;
    private Integer rowsToSkip = 0;
    private Map<String, String> deleteConditions; // only applies to this reader for now - any rows which match
                                                  // these criteria will become a delete instead of insert

    /**
     * Event Hub config properties
     */
    private Map<String, String> jsonPaths;
    private String apiVersion = "1.0.0";
    private String jmsEventKey;
    private String alternativeServiceName;
    private Boolean overrideServiceName = false;
    private String environment;
    private String prefixEventId;
    private String eventId;

    /**
     * ShiftLab Mongo DB properties
     */
    private LinkedHashMap<String, String> bindTransactionValues;

    /**
     * AWS S3 config properties
     */
    private String bucket;
    private String s3FileName;
    private Integer maxKeys = 10000;
    private Integer lastModifiedDelayHours = 72;

    /**
     * Mongo DB properties
     */
    private List<String> flattenPath;
    private Boolean useMongoExtendedDate = true;
    private String mongoDatabase;
    private String mongoCollection;
    private String mongoQuery;
    // private UuidRepresentation uuidRepresentation =
    // UuidRepresentation.UNSPECIFIED;

    // Rest Templates
    private Boolean useTlsV3RestTemplate = false;
    private String soqlQuery;

    @JsonProperty(value = "isRedisConsumerStream")
    public Boolean isRedisConsumerStream() {
        return this.isRedisConsumerStream;
    }

    @JsonProperty(value = "isCassandraCluster")
    public Boolean isCassandraCluster() {
        return this.isCassandraCluster;
    }

    @JsonProperty(value = "isBulk")
    public Boolean isBulk() {
        return this.isBulk;
    }

    @JsonProperty(value = "allowFiltering")
    public Boolean allowFiltering() {
        return this.allowFiltering;
    }

    @JsonProperty(value = "isJmsRespectTTL")
    public Boolean isJmsRespectTTL() {
        return this.isJmsRespectTTL;
    }

    @JsonProperty(value = "isMsgQueueEmpty")
    public Boolean isMsgQueueEmpty() {
        return this.isMsgQueueEmpty;
    }

    @JsonProperty(value = "enableSSL")
    public Boolean isEnableSSL() {
        return this.enableSSL;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ReaderConfig(");
        if (reader != null)
            sb.append("reader=").append(reader).append(", ");
        if (readerType != null)
            sb.append("readerType=").append(readerType).append(", ");
        if (readerBuilder != null)
            sb.append("readerBuilder=").append(readerBuilder).append(", ");
        if (tableName != null)
            sb.append("tableName=").append(tableName).append(", ");
        if (connectionName != null)
            sb.append("connectionName=").append(connectionName).append(", ");
        if (selectClause != null)
            sb.append("selectClause=").append(selectClause).append(", ");
        if (fromClause != null)
            sb.append("fromClause=").append(fromClause).append(", ");
        if (whereClause != null && !whereClause.isEmpty())
            sb.append("whereClause=").append(whereClause).append(", ");
        if (sb.length() > 13)
            sb.setLength(sb.length() - 2);
        sb.append(")");
        return sb.toString();
    }
}