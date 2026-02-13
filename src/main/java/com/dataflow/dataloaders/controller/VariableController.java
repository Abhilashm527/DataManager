package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.util.Response;
import com.dataflow.dataloaders.entity.Variable;
import com.dataflow.dataloaders.entity.VariableGroup;
import com.dataflow.dataloaders.services.VariableService;
import com.dataflow.dataloaders.util.Identifier;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/v1/variables")
@Tag(name = "Variables", description = "Variable and Secret Management")
public class VariableController {

    @Autowired
    private VariableService variableService;

    @Operation(summary = "Get variables context", description = "Fetch Global and App-specific variables")
    @GetMapping("/context")
    public ResponseEntity<Response> getContext(
            @RequestParam(required = false) String applicationId,
            @RequestParam(required = false) String environment) {
        return Response.getResponse(variableService.getVariableContext(applicationId, environment));
    }

    @Operation(summary = "Create variable group")
    @PostMapping("/groups")
    public ResponseEntity<Response> createGroup(@RequestBody VariableGroup group, @RequestHeader HttpHeaders headers) {
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(variableService.createGroup(group, identifier));
    }

    @Operation(summary = "Update variable group")
    @PutMapping("/groups/{id}")
    public ResponseEntity<Response> updateGroup(@PathVariable String id, @RequestBody VariableGroup group) {
        group.setId(id);
        return Response.getResponse(variableService.updateGroup(group));
    }

    @Operation(summary = "Delete variable group")
    @DeleteMapping("/groups/{id}")
    public ResponseEntity<Response> deleteGroup(@PathVariable String id,
            @RequestHeader(value = "user", defaultValue = "admin") String user) {
        variableService.deleteGroup(id, user);
        return Response.getResponse("Group deleted successfully");
    }

    @Operation(summary = "Create variable")
    @PostMapping()
    public ResponseEntity<Response> createVariable(@RequestBody Variable variable, @RequestHeader HttpHeaders headers) {
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(variableService.createVariable(variable, identifier));
    }

    @Operation(summary = "Update variable")
    @PutMapping("/{id}")
    public ResponseEntity<Response> updateVariable(@PathVariable String id, @RequestBody Variable variable) {
        variable.setId(id);
        return Response.getResponse(variableService.updateVariable(variable));
    }

    @Operation(summary = "Delete variable")
    @DeleteMapping("/{id}")
    public ResponseEntity<Response> deleteVariable(@PathVariable String id,
            @RequestHeader(value = "user", defaultValue = "admin") String user) {
        variableService.deleteVariable(id, user);
        return Response.getResponse("Variable deleted successfully");
    }
}
