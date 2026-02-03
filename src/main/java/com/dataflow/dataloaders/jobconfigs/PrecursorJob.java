package com.dataflow.dataloaders.jobconfigs;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class PrecursorJob {
    private String jobName;
    private Long minutes;
}
