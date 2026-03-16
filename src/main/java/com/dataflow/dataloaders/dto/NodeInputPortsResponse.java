package com.dataflow.dataloaders.dto;

import com.dataflow.dataloaders.entity.dagmodels.dag.Node;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

/**
 * Response DTO for connected input ports of a node.
 * Contains the output ports from each upstream (source) node connected via edges —
 * these become the available input fields for the target node.
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NodeInputPortsResponse {

    private String targetNodeId;
    private List<SourceNodePorts> connectedSources;

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SourceNodePorts {
        private String sourceNodeId;
        private String sourceNodeName;
        private Node.NodeType sourceNodeType;
        private String edgeId;
        private String sourcePort;
        private String targetPort;
        /** Output ports from the source node — these are the fields available as inputs for the target node */
        private List<Node.Port> outputPorts;
    }
}
