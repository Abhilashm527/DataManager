package com.dataflow.dataloaders.entity.dag;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service responsible for propagating and inferring schemas across a DAG.
 */
@Slf4j
@Service
public class DAGSchemaService {

    /**
     * Propagates schemas from sources to destinations across the entire DAG.
     */
    public void propagateSchemas(DAGDefinition dag) {
        log.info("Starting schema propagation for DAG: {}", dag.getDagId());

        // 0. Pre-process: Reverse Inference (Guess Reader fields based on downstream
        // Mappers)
        reverseInferencePass(dag);

        // 1. Identify source nodes (nodes with no incoming edges or Reader types)
        List<Node> sourceNodes = dag.getNodes().stream()
                .filter(this::isSourceNode)
                .collect(Collectors.toList());

        Set<String> processedNodes = new HashSet<>();
        Queue<Node> queue = new LinkedList<>(sourceNodes);

        // 2. Breadth-first traversal to propagate schema
        while (!queue.isEmpty()) {
            Node currentNode = queue.poll();
            if (currentNode == null)
                continue;

            // Compute output schema for current node
            Schema outputSchema = computeOutputSchema(currentNode, dag);
            currentNode.setNodeSchema(outputSchema); // Set direct node schema
            updateNodeOutputPorts(currentNode, outputSchema);

            processedNodes.add(currentNode.getNodeId());

            // Find downstream nodes
            List<Edge> outgoingEdges = dag.getEdges().stream()
                    .filter(e -> e.getSourceNodeId().equals(currentNode.getNodeId()))
                    .collect(Collectors.toList());

            for (Edge edge : outgoingEdges) {
                Node targetNode = findNode(dag, edge.getTargetNodeId());
                if (targetNode != null) {
                    // Apply edge transformation to the schema before passing it
                    Schema transformedSchema = applyEdgeMapping(outputSchema, edge);
                    updateNodeInputPorts(targetNode, transformedSchema);

                    // Add to queue if all dependencies are met (basic check)
                    if (!processedNodes.contains(targetNode.getNodeId())) {
                        queue.add(targetNode);
                    }
                }
            }
        }
        log.info("Schema propagation completed for DAG: {}", dag.getDagId());
    }

    private boolean isSourceNode(Node node) {
        String type = node.getNodeType().name();
        return type.endsWith("READER") || type.equals("TRIGGER")
                || (node.getDependsOn() == null || node.getDependsOn().isEmpty());
    }

    private Node findNode(DAGDefinition dag, String nodeId) {
        return dag.getNodes().stream()
                .filter(n -> n.getNodeId().equals(nodeId))
                .findFirst()
                .orElse(null);
    }

    /**
     * Look ahead at downstream Mappers to see what fields they expect.
     * This fulfills the requirement of showing "what should be flowing" even if not
     * explicitly defined.
     */
    private void reverseInferencePass(DAGDefinition dag) {
        for (Edge edge : dag.getEdges()) {
            Node sourceNode = findNode(dag, edge.getSourceNodeId());
            Node targetNode = findNode(dag, edge.getTargetNodeId());

            if (sourceNode != null && targetNode != null && targetNode.getNodeType() == Node.NodeType.MAPPER
                    && targetNode.getMapperConfig() != null) {

                // If source is a Reader, we can inject the expected fields
                if (sourceNode.getNodeType().name().endsWith("READER")) {
                    if (sourceNode.getNodeSchema() == null) {
                        sourceNode.setNodeSchema(new Schema());
                        sourceNode.getNodeSchema().setNodeId(sourceNode.getNodeId());
                        sourceNode.getNodeSchema().setFields(new ArrayList<>());
                        sourceNode.getNodeSchema().setName(sourceNode.getNodeName() + "_Inferred");
                    }

                    List<FieldDefinition> sourceFields = sourceNode.getNodeSchema().getFields();
                    for (Node.FieldMapperConfig.MappingEntry mapping : targetNode.getMapperConfig().getMappings()) {
                        String sourcePath = mapping.getSourcePath();
                        if (sourcePath != null && !sourcePath.equals("(expression)") && !sourcePath.contains(".")) {
                            // Check if field already exists
                            boolean exists = sourceFields.stream().anyMatch(f -> f.getName().equals(sourcePath));
                            if (!exists) {
                                FieldDefinition inferredField = new FieldDefinition();
                                inferredField.setName(sourcePath);
                                inferredField.setType(FieldDefinition.DataType.STRING); // Default
                                inferredField.addTrace("Inferred from downstream Mapper: " + targetNode.getNodeId());

                                // Set initial lineage
                                FieldDefinition.Lineage lineage = new FieldDefinition.Lineage();
                                lineage.setSourceNodeId(sourceNode.getNodeId());
                                lineage.setSourcePath(sourcePath);
                                lineage.setFlowTrace(new ArrayList<>(
                                        Collections.singletonList("Inferred via Reverse Mapping Analysis")));
                                inferredField.setLineage(lineage);

                                sourceFields.add(inferredField);
                            }
                        }
                    }
                }
            }
        }
    }

    private Schema computeOutputSchema(Node node, DAGDefinition dag) {
        // If it's a reader, we might already have a schema from DDL
        // If it's a processor, it likely transforms the input schema

        Schema inputSchema = null;
        if (node.getInputPorts() != null && !node.getInputPorts().isEmpty()) {
            inputSchema = node.getInputPorts().get(0).getSchema();
        }

        Schema outputSchema = new Schema();
        outputSchema.setNodeId(node.getNodeId());
        outputSchema.setCapturedAt(Instant.now());
        outputSchema.setName(node.getNodeName() + "_Output");

        if (inputSchema == null) {
            // Reader type - should have fields defined in config or inferred from DDL
            outputSchema.setFields(inferReaderFields(node));
        } else {
            // Processor type - apply node-specific logic (Filter, Aggregator, etc.)
            outputSchema.setFields(applyNodeTransformation(inputSchema, node));
        }

        return outputSchema;
    }

    private List<FieldDefinition> inferReaderFields(Node node) {
        // 1. Check direct node schema (this is where Reverse Inference or explicit JSON
        // input puts it)
        if (node.getNodeSchema() != null && node.getNodeSchema().getFields() != null
                && !node.getNodeSchema().getFields().isEmpty()) {
            return node.getNodeSchema().getFields();
        }

        // 2. Fallback to Port definition
        if (node.getOutputPorts() != null && !node.getOutputPorts().isEmpty()
                && node.getOutputPorts().get(0).getSchema() != null) {
            return node.getOutputPorts().get(0).getSchema().getFields();
        }
        return new ArrayList<>();
    }

    private List<FieldDefinition> applyNodeTransformation(Schema inputSchema, Node node) {
        List<FieldDefinition> outputFields = new ArrayList<>();

        if (node.getNodeType() == Node.NodeType.MAPPER && node.getMapperConfig() != null) {
            // Visual Field Mapper Logic: Create fields based on explicit mappings
            for (Node.FieldMapperConfig.MappingEntry mapping : node.getMapperConfig().getMappings()) {
                FieldDefinition sourceField = findFieldByPath(inputSchema, mapping.getSourcePath());
                FieldDefinition newField = new FieldDefinition();

                // 1. Set Name from target path
                newField.setName(extractNameFromPath(mapping.getTargetPath()));

                // 2. Handle Lineage and Type
                if (sourceField != null) {
                    newField.setType(sourceField.getType());
                    newField.setProperties(
                            sourceField.getProperties() != null ? new HashMap<>(sourceField.getProperties()) : null);

                    // Copy Lineage
                    FieldDefinition.Lineage lineage = new FieldDefinition.Lineage();
                    if (sourceField.getLineage() != null) {
                        lineage.setSourceNodeId(sourceField.getLineage().getSourceNodeId());
                        lineage.setSourcePath(sourceField.getLineage().getSourcePath());
                        lineage.setFlowTrace(new ArrayList<>(sourceField.getLineage().getFlowTrace()));
                    } else {
                        // Fallback if source field has no lineage yet
                        lineage.setSourcePath(sourceField.getName());
                        lineage.setFlowTrace(new ArrayList<>());
                    }
                    newField.setLineage(lineage);
                } else {
                    // Virtual field or Expression (the (expression) part of your screenshot)
                    newField.setType(FieldDefinition.DataType.STRING);
                    FieldDefinition.Lineage lineage = new FieldDefinition.Lineage();
                    lineage.setSourceNodeId(node.getNodeId());
                    lineage.setSourcePath(mapping.getSourcePath());
                    newField.setLineage(lineage);
                }

                // 3. Add Transformation Trace (The middle blocks in your screenshot like
                // 'Conc', 'Trim')
                if (mapping.getTransforms() != null && !mapping.getTransforms().isEmpty()) {
                    newField.addTrace("Transforms: " + String.join(", ", mapping.getTransforms()));
                }

                newField.addTrace("Mapped to " + mapping.getTargetPath() + " in " + node.getNodeName());
                outputFields.add(newField);
            }
        } else {
            // Standard Processor (Enricher, Filter, etc.): Deep copy and trace
            for (FieldDefinition field : inputSchema.getFields()) {
                FieldDefinition newField = copyField(field);
                newField.addTrace("Processed by node: " + node.getNodeId() + " (" + node.getNodeType() + ")");
                outputFields.add(newField);
            }

            // Logic for specific processors
            if (node.getNodeType() == Node.NodeType.FILTER) {
                // Filters don't usually change structure, just mark them
                outputFields.forEach(f -> f.addTrace("Passed through Filter"));
            }
        }

        return outputFields;
    }

    private Schema applyEdgeMapping(Schema sourceSchema, Edge edge) {
        if (edge.getTransformation() == null || edge.getTransformation().getMappings() == null
                || edge.getTransformation().getMappings().isEmpty()) {
            return sourceSchema; // Direct passthrough
        }

        Schema targetSchema = new Schema();
        targetSchema.setNodeId(edge.getTargetNodeId());
        targetSchema.setCapturedAt(Instant.now());
        List<FieldDefinition> mappedFields = new ArrayList<>();

        for (Edge.FieldMapping mapping : edge.getTransformation().getMappings()) {
            FieldDefinition sourceField = findFieldByPath(sourceSchema, mapping.getSourcePath());
            if (sourceField != null) {
                FieldDefinition mappedField = copyField(sourceField);

                // Handle RENAME or Path changes
                if (mapping.getTargetPath() != null) {
                    mappedField.setName(extractNameFromPath(mapping.getTargetPath()));
                }

                mappedField.addTrace("Mapped via edge: " + edge.getEdgeId());
                mappedFields.add(mappedField);
            }
        }

        targetSchema.setFields(mappedFields);
        return targetSchema;
    }

    private void updateNodeOutputPorts(Node node, Schema schema) {
        if (node.getOutputPorts() == null) {
            node.setOutputPorts(new ArrayList<>());
            Node.Port defaultPort = new Node.Port();
            defaultPort.setPortId("default-out");
            defaultPort.setPortName("Output Port");
            node.getOutputPorts().add(defaultPort);
        }
        node.getOutputPorts().forEach(p -> p.setSchema(schema));
    }

    private void updateNodeInputPorts(Node node, Schema schema) {
        if (node.getInputPorts() == null) {
            node.setInputPorts(new ArrayList<>());
            Node.Port defaultPort = new Node.Port();
            defaultPort.setPortId("default-in");
            defaultPort.setPortName("Input Port");
            node.getInputPorts().add(defaultPort);
        }
        node.getInputPorts().forEach(p -> p.setSchema(schema));
    }

    public List<Map<String, Object>> generateLineageReport(DAGDefinition dag) {
        propagateSchemas(dag); // Ensure schemas are fresh

        List<Map<String, Object>> report = new ArrayList<>();

        // Find all "Writer" or "Leaf" nodes (nodes with no outgoing edges)
        List<Node> leafNodes = dag.getNodes().stream()
                .filter(n -> dag.getEdges().stream().noneMatch(e -> e.getSourceNodeId().equals(n.getNodeId())))
                .collect(Collectors.toList());

        for (Node leaf : leafNodes) {
            if (leaf.getNodeSchema() != null && leaf.getNodeSchema().getFields() != null) {
                for (FieldDefinition field : leaf.getNodeSchema().getFields()) {
                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("destinationNode", leaf.getNodeId());
                    entry.put("targetField", field.getName());

                    if (field.getLineage() != null) {
                        entry.put("sourceNode", field.getLineage().getSourceNodeId());
                        entry.put("sourceField", field.getLineage().getSourcePath());
                        entry.put("history", field.getLineage().getFlowTrace());
                    }
                    report.add(entry);
                }
            }
        }
        return report;
    }

    private FieldDefinition findFieldByPath(Schema schema, String path) {
        // Simple dot-notation search
        if (path == null || schema.getFields() == null)
            return null;
        String[] parts = path.split("\\.");

        List<FieldDefinition> currentLevel = schema.getFields();
        FieldDefinition found = null;

        for (String part : parts) {
            found = currentLevel.stream()
                    .filter(f -> f.getName().equals(part))
                    .findFirst()
                    .orElse(null);

            if (found == null)
                return null;
            currentLevel = found.getChildren();
        }
        return found;
    }

    private String extractNameFromPath(String path) {
        if (!path.contains("."))
            return path;
        return path.substring(path.lastIndexOf(".") + 1);
    }

    private FieldDefinition copyField(FieldDefinition original) {
        FieldDefinition copy = new FieldDefinition();
        copy.setName(original.getName());
        copy.setType(original.getType());
        copy.setProperties(original.getProperties() != null ? new HashMap<>(original.getProperties()) : null);

        if (original.getLineage() != null) {
            FieldDefinition.Lineage newLineage = new FieldDefinition.Lineage();
            newLineage.setSourceNodeId(original.getLineage().getSourceNodeId());
            newLineage.setSourcePath(original.getLineage().getSourcePath());
            newLineage.setFlowTrace(new ArrayList<>(original.getLineage().getFlowTrace()));
            copy.setLineage(newLineage);
        } else {
            // First time tracking lineage
            FieldDefinition.Lineage lineage = new FieldDefinition.Lineage();
            lineage.setSourcePath(original.getName());
            copy.setLineage(lineage);
        }

        if (original.getChildren() != null) {
            copy.setChildren(original.getChildren().stream().map(this::copyField).collect(Collectors.toList()));
        }
        return copy;
    }
}
