package com.dataflow.dataloaders.jobconfigs;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;

@Component
@Getter
@Setter
@ToString
public class ConnectionConfig {

    private List<String> cassandraHosts;
    /**
     * IP address of bastion (used for azure -> on-prem connectivity)
     */
    private String bastionIp;

    private List<String> cassandraHostnames;
    private Integer cassandraPort;
    private String cassandraPassword;
    private String cassandraUser;
    private String private_key;

    private String sftpHost;
    private Integer sftpPort;
    private String sftpUser;
    private String sftpPassword;
    private String sftpRemoteFile;
    private String sftpPrivateKey;
    private String sftpPrivateKeyPassphrase;
    private String privateKey;


    /**
     * Snowflake Props
     */
    private String snowflakeUrl;
    private String snowflakeDriverName;
    private String snowflakeUser;
    private String snowflakePassword;
    private String snowflakePrivateKey;
    private String snowflakeDb;
    private String snowflakeSchema;
    private String snowflakeAccount;

    /**
     * Salesforce Props
     */
    private String salesforceUsername;
    private String salesforceConsumerKey;
    private String salesforceTokenEndpoInteger;
    private String salesforceAudience;
    private String salesforcePrincipal;
    private String salesforceKeystorePath;
    private String salesforceKeystorePassword;
    private String salesforcePassword;
    private String salesforceConsumerSecret;
    private String salesforceKeystoreAlias;

    /**
     * Azure blob connections
     */
    private String azureBlobConnectionString; // The full Azure Blob Storage connection string

    /**
     //     JDBC Reader config Properties
     //     */
    private String jdbcUrl;
    private String jdbcDriverName;
    private String jdbcUser;
    private String jdbcPassword;

    private String redisHostname;
    private Integer redisPort;
    private Integer redisDatabase;
    private String redisPassword;
    private Boolean withverifypeer = false;
    private String tlsCertificate;
    private Boolean ssl = false;

    private String JmsHostname;
    private String JmsUsername;
    private String JmsPassword;
    private String JmsVpn;
    private String JmsClientId = "";

    private String elasticsearchHostname;
    private Integer elasticsearchPort;
    private String elasticsearchUsername;
    private String elasticsearchPassword;

    /**
     AWS S3 connection
     */
    private String accessKey;
    private String secretKey;
    private String region;

    private String sambaUser;
    private String sambaPassword;
    private String sambaDomain;
    private String sambaPath;

    private String mongoConnectionString;

    private String sapUser;
    private String sapPassword;
    
    // OAuth 2.0 fields for SAP Cloud
    private String sapOAuthTokenUrl;
    private String sapOAuthClientId;
    private String sapOAuthClientSecret;
    private String sapOAuthScope;
    
    // API Key for SAP API Hub Sandbox
    private String sapApiKey;
    
    public String getSapApiKey() {
        return sapApiKey;
    }
    
    public void setSapApiKey(String sapApiKey) {
        this.sapApiKey = sapApiKey;
    }

    private List<LinkedHashMap<String, LinkedHashMap<String, Object>>> clusterNodes;

}
