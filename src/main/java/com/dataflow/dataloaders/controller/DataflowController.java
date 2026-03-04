package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.dto.SearchPayload;
import com.dataflow.dataloaders.entity.Dataflow;
import com.dataflow.dataloaders.services.DataflowService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.*;

import static com.dataflow.dataloaders.config.APIConstants.DATAFLOWS_BASE_PATH;
import static com.dataflow.dataloaders.util.QueryUtil.getSearchQuery;

@Slf4j
@RestController
@RequestMapping(DATAFLOWS_BASE_PATH)
@Tag(name = "Dataflows", description = "Dataflow management APIs")
public class DataflowController {

    @Autowired
    private DataflowService dataflowService;

    @Operation(summary = "Create dataflow")
    @PostMapping
    public ResponseEntity<Response> create(@RequestBody Dataflow dataflow, @RequestHeader HttpHeaders headers) {
        log.info("Creating dataflow");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(dataflowService.create(dataflow, identifier));
    }

    @Operation(summary = "Get dataflow by ID")
    @GetMapping("/{dataflowId}")
    public ResponseEntity<Response> get(@Parameter(description = "Dataflow ID") @PathVariable String dataflowId,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting dataflow: {}", dataflowId);
        Identifier identifier = Identifier.builder().headers(headers).word(dataflowId).build();
        return Response.getResponse(dataflowService.getDataflow(identifier));
    }

    @Operation(summary = "Get dataflows by application ID with pagination")
    @PostMapping("/application/{applicationId}/list")
    public ResponseEntity<Response> getByApplicationId(
            @Parameter(description = "Application ID") @PathVariable String applicationId,
            @Valid @RequestBody(required = false) SearchPayload searchPayload,
            @RequestParam(required = false) Boolean isFavorite,
            @RequestParam(required = false) String search,
            @RequestParam(required = false, defaultValue = "0") int page,
            @RequestParam(required = false, defaultValue = "10") int size,
            @RequestParam(required = false, defaultValue = "created_at") String sortBy,
            @RequestParam(required = false, defaultValue = "DESC") Sort.Direction direction,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting dataflows by application: {}", applicationId);
        Pageable pageable = PageRequest.of(page, size, Sort.by(direction, sortBy));
        Identifier identifier = Identifier.builder().headers(headers).pageable(pageable).build();

        StringBuilder searchQuery = new StringBuilder(" AND application_id = '" + applicationId + "'");
        if (isFavorite != null) {
            searchQuery.append(" AND is_favorite = ").append(isFavorite);
        }
        if (search != null && !search.isEmpty()) {
            searchQuery.append(" AND dataflow_name ILIKE '%").append(search).append("%'");
        }
        if (!ObjectUtils.isEmpty(searchPayload) && !CollectionUtils.isEmpty(searchPayload.getSearchCriterias())) {
            searchQuery.append(getSearchQuery(searchPayload.getSearchCriterias()));
        }
        identifier.setWord(searchQuery.toString());

        return Response.getResponse(dataflowService.list(identifier));
    }

    @Operation(summary = "Update dataflow")
    @PutMapping("/{dataflowId}")
    public ResponseEntity<Response> update(@Parameter(description = "Dataflow ID") @PathVariable String dataflowId,
            @RequestBody Dataflow dataflow,
            @RequestHeader HttpHeaders headers) {
        log.info("Updating dataflow: {}", dataflowId);
        Identifier identifier = Identifier.builder().headers(headers).word(dataflowId).build();
        return Response.updateResponse(dataflowService.updateDataflow(dataflow, identifier));
    }

    @Operation(summary = "Delete dataflow")
    @DeleteMapping("/{dataflowId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Dataflow ID") @PathVariable String dataflowId,
            @RequestHeader HttpHeaders headers) {
        log.info("Deleting dataflow: {}", dataflowId);
        Identifier identifier = Identifier.builder().headers(headers).word(dataflowId).build();
        return Response.deleteResponse(dataflowService.deleteDataflow(identifier));
    }

    @Operation(summary = "Toggle favorite status")
    @PatchMapping("/{dataflowId}/favorite")
    public ResponseEntity<Response> toggleFavorite(
            @Parameter(description = "Dataflow ID") @PathVariable String dataflowId,
            @RequestParam Boolean isFavorite,
            @RequestHeader HttpHeaders headers) {
        log.info("Toggling favorite status for dataflow: {} to {}", dataflowId, isFavorite);
        Identifier identifier = Identifier.builder().headers(headers).word(dataflowId).build();
        Dataflow update = new Dataflow();
        update.setIsFavorite(isFavorite);
        return Response.updateResponse(dataflowService.updateDataflow(update, identifier));
    }
}
