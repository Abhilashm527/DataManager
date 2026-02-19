package com.dataflow.dataloaders.jobconfigs;

//import com.azure.storage.blob.BlobClient;
//import com.azure.storage.blob.BlobContainerClient;
//import com.datastax.driver.core.Session;
//import com.ivoyant.model.StreamMetadata;
//import com.ivoyant.model.registry.JobRegistryEntry;
//import com.ivoyant.rest.SalesforceAuthProvider;
//import com.jcraft.jsch.ChannelSftp;
import lombok.Getter;
import lombok.Setter;
//import org.springframework.batch.core.StepExecution;
//import org.springframework.batch.item.file.transform.LineAggregator;
//import org.springframework.integration.file.remote.session.CachingSessionFactory;
//import org.springframework.integration.sftp.inbound.SftpStreamingMessageSource;
//import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
//import org.springframework.integration.sftp.session.SftpSession;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Setter
@Getter
@Component("jobConfigsInternalConfig")
public class InternalConfig {
    /**
     * This class is used internally to allow settings to be propagated throughout
     * the job process.
     * These fields should never be included in the yaml config. They are set
     * internally only.
     */

    private byte[] fileContent;
    // private S3ObjectSummary s3ObjectSummary;

    // private SftpStreamingMessageSource sftpStreamingMessageSource;
    // private SftpRemoteFileTemplate sftpTemplate;
    // private CachingSessionFactory<ChannelSftp.LsEntry> cachingSessionFactory;
    // private SftpSession readerSftpSession;

    // private SmbFileOutputStream smbFileOutputStream;

    /**
     * remote partition
     */
    private String stepName = "";
    private int minValue;
    private int maxValue;
    // private StepExecution stepExecution;

    /**
     * Snowflake connection
     */

    /**
     * database connections
     */
    // private Session cassandraWriterSession;
    // private Session cassandraReaderSession;
    // private Session cassandraProcessorSession;
    private DriverManagerDataSource jdbcReaderDataSource;
    private DriverManagerDataSource jdbcWriterDataSource;
    private DriverManagerDataSource jdbcRestFlagWriterDataSource;
    private DriverManagerDataSource jdbcProcessorDataSource;
    // private BlobContainerClient blobContainerClient;
    // private BlobContainerClient writerDestiationBlobContainerClient;
    // private BlobContainerClient writerSourceBlobContainerClient;

    // Azure Blob client
    // private BlobClient azureBlobClient;
    // // Salesforce Auth Providers for reader, writer, processor
    // private SalesforceAuthProvider readerSalesforceAuthProvider;
    // private SalesforceAuthProvider writerSalesforceAuthProvider;
    // private SalesforceAuthProvider processorSalesforceAuthProvider;

    private RestTemplate sapRestTemplate;
    // private StatefulRedisConnection redisReaderConnection;
    // private StatefulRedisConnection redisWriterConnection;
    // private RedisClient redisReaderClient;
    // private RedisClient redisWriterClient;
    // private StatefulRedisClusterConnection redisClusterWriterConnection;

    // private RestHighLevelClient elasticsearchReaderClient;
    // private RestHighLevelClient elasticsearchWriterClient;

    // private Connection jmsReaderConnection;
    // private Connection jmsWriterConnection;
    // private javax.jms.Session jmsSession;
    // private MessageProducer jmsMessageProducer;
    // private MessageConsumer jmsMessageConsumer;

    // private HashMap<String, StatefulRedisConnection> redisReaderConnections = new
    // HashMap<>();
    // private HashMap<String, StatefulRedisConnection> redisWriterConnections = new
    // HashMap<>();

    // @JsonIgnore
    // private AmazonS3Client amazonS3Client;

    // private MongoClient mongoClient;

    /**
     * Used for job scheduling
     */
    private String jobName;
    private Long schedulerId;
    private Long jobId;

    private long maxChunkEstimate;

    private ConcurrentHashMap<Object, Integer> concurrentHashMap = new ConcurrentHashMap<>();
    private Set<Object> removeDuplicatesSet = concurrentHashMap.newKeySet();

    private boolean async = true;

    /**
     * used in Filter Processor in ROW_CONTAINS switch case
     */
    private boolean rowContains = false;
    private Integer totalRecords = null;

    private boolean noFileFound = false;
    private boolean noPermissionFound = false;
    private String permissionDeniedLocation;

    /**
     * Used for keeping data from a beforeJob to use later in job
     **/
    private List<LinkedHashMap<String, Object>> internalData;

    /**
     * Used in Redis Hash Command Executor for conditional updates
     */
    private List<String> mappedInputFields;

    /**
     * File writer properties
     */
    private Integer linesWritten = 0;
    // private LineAggregator lineAggregator;
    private String formattedDate;

    public void incrementTotalRecords() {
        if (totalRecords == null) {
            totalRecords = 1;
        } else {
            totalRecords++;
        }
    }

    public void incrementLinesWritten() {
        if (linesWritten == null) {
            linesWritten = 1;
        } else {
            linesWritten++;
        }
    }

}
