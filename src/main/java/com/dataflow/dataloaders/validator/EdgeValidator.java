package com.dataflow.dataloaders.validator;

import com.dataflow.dataloaders.entity.dagmodels.dag.Edge;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.services.dag.NodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class EdgeValidator {

    @Autowired
    private NodeService nodeService;

    public void validate(Edge edge) {
        if (edge == null) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, "Edge cannot be null");
        }

        if (edge.getDataflowId() == null || edge.getDataflowId().trim().isEmpty()) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, "Dataflow ID is required for an edge");
        }

        if (edge.getSourceNodeId() == null || edge.getSourceNodeId().trim().isEmpty()) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, "Source node ID is required");
        }

        if (edge.getTargetNodeId() == null || edge.getTargetNodeId().trim().isEmpty()) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, "Target node ID is required");
        }

        // Validate source node exists
        if (nodeService.getNodeById(edge.getSourceNodeId()).isEmpty()) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST,
                    String.format("Source node with ID '%s' does not exist", edge.getSourceNodeId()));
        }

        // Validate target node exists
        if (nodeService.getNodeById(edge.getTargetNodeId()).isEmpty()) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST,
                    String.format("Target node with ID '%s' does not exist", edge.getTargetNodeId()));
        }

        if (edge.getSourceNodeId().equals(edge.getTargetNodeId())) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, "Source and target nodes cannot be the same");
        }
    }
}
