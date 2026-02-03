package com.dataflow.dataloaders.jobconfigs;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.stereotype.Component;

import java.util.List;

@Getter
@Setter
@Component
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobNotes {

    public enum SeverityEnum{
        Critical,
        High,
        Medium,
        Low
    }
    private SeverityEnum severity;
    private String jobDescription;
    private List<String> impacts;
    private String failureNotes;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("JobNotes(");
        if (severity != null) sb.append("severity=").append(severity).append(", ");
        if (jobDescription != null) sb.append("jobDescription=").append(jobDescription).append(", ");
        if (impacts != null) sb.append("impacts=").append(impacts).append(", ");
        if (sb.length() > 9) sb.setLength(sb.length() - 2);
        sb.append(")");
        return sb.toString();
    }

}
