package com.dataflow.dataloaders.jobconfigs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.stereotype.Component;

import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.List;

@Getter
@Setter
@Component
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeployJobBundle {

    private String configName = "unknown config";
    private Integer chunkSize;
    private Integer customWriteSize = 10000;
    private Boolean saveState = false;
    private Boolean multithreaded = false;
    private Boolean remotePartition = false;
    private Integer numberOfPartitions = 2;
    private LinkedHashMap<String, String> partitionDetails;
    private String startRow;
    private String endRow;
    private String partitionColumn;
    private String partitioner;
    private Boolean castPartitionColumn = true;
    private List<String> partitionNames;
    private Boolean processBeforeValues; //this has to be false for jobs which have deletes where we have multiple data structures based on conditions within the row
    //need to come back to this later, fix delete logic to include conditions based on previousValues, then remove this param and always have as true
    //example: 2 hashes are written for a particular cases, each with a condition for a different status.. delete comes with status A,
    //but both hashes will be deleted since the condition is not checked upon delete

    /**
     * Scheduler options
     */
    private Boolean scheduled = false;
    private String schedule;
    private String retrySchedule;
    private Boolean autoRestart = false;
    private Boolean useLastRunParam = false;
    private String dataloaderIdOverride;
    private Long lastRunParam;
    private String zeroRecordSchedule;
    private String cutOffDate;
    private List<PrecursorJob> precursorJobs;

    /**
     * Processor Connections (only used in very specific use-cases, such as GSMA)
     */
    private String processorConnectionName;
    private ConnectionConfig processorConnectionConfig;
    private Boolean alertOnFileNotFound=false;
    private LinkedHashMap<String,Object> customProcessors;
    private LinkedHashMap<String, LinkedHashMap<PROCESSOR_ENUM,Object>> rowProcessors;
    private List<InputField> inputFields;
    private List<InputField> newFields;
    private ReaderConfig readerConfig;
    private WriterConfig writerConfig;
    private List<WriterConfig> compositeWriterConfig;
    @JsonIgnore
    private InternalConfig integerernalConfig = new InternalConfig();

    private Boolean removeDuplicates;
    private LinkedHashMap<PROCESSOR_ENUM,Object> removeDuplicatesArguments;
    private Boolean modifyKeys;

    private ReaderConfig.DateBindType bindDate;
    private Integer bindDateArgument;

    private List<String> scriptCommands;
    private Boolean kerberosEnabled = false;
    private String additionalEmails=null;


    /**
     * Before/After job
     */
    private String function;
//    private BeforeJobConfig beforeJobConfig;
//    private AfterJobConfig afterJobConfig;
    private Long minimumRowCount ;
    private Long actualRowCount ;

    //used to reduce the amount of logging for a job
    private Integer logFrequency = 1;

    //used to determine if a job should be managed in PerpetualJobs
    private Boolean perpetualJob = false;

    /**
     * jobNotes
     **/
    private JobNotes jobNotes;

    public DeployJobBundle() { }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DeployJobBundle(");
        if (configName != null) sb.append("configName=").append(configName).append(", ");
        if (chunkSize != null) sb.append("chunkSize=").append(chunkSize).append(", ");
        if (customWriteSize != null) sb.append("customWriteSize=").append(customWriteSize).append(", ");
        if (saveState != null) sb.append("saveState=").append(saveState).append(", ");
        if (multithreaded != null) sb.append("multithreaded=").append(multithreaded).append(", ");
        if (remotePartition != null) sb.append("remotePartition=").append(remotePartition).append(", ");
        if (numberOfPartitions != null) sb.append("numberOfPartitions=").append(numberOfPartitions).append(", ");
        if (scheduled != null) sb.append("scheduled=").append(scheduled).append(", ");
        if (schedule != null) sb.append("schedule=").append(schedule).append(", ");
        if (inputFields != null) sb.append("inputFields=").append(inputFields).append(", ");
        if (readerConfig != null) sb.append("readerConfig=").append(readerConfig).append(", ");
        if (writerConfig != null) sb.append(writerConfig).append(", ");
        if (jobNotes != null) sb.append("jobNotes=").append(jobNotes).append(", ");
        if (sb.length() > 16) sb.setLength(sb.length() - 2); // Remove last ", "
        sb.append(")");
        return sb.toString();
    }
}

/*    @JsonProperty(value="removeDuplicates")
    public Boolean isRemoveDuplicates() {
        return removeDuplicates;
    }*/
