package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.dto.SearchPayload;
import com.dataflow.dataloaders.entity.Datatable;
import com.dataflow.dataloaders.entity.DataTableRecord;
import com.dataflow.dataloaders.services.DatatableService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
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

import java.util.List;

import static com.dataflow.dataloaders.util.QueryUtil.getSearchQuery;

import static com.dataflow.dataloaders.config.APIConstants.DATATABLE_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(DATATABLE_BASE_PATH)
@Tag(name = "Datatable", description = "Datatable management APIs with Postgres (Schema) and MongoDB (Records)")
public class DatatableController {
    protected static final String logPrefix = "{} : {}";

    @Autowired
    private DatatableService datatableService;

    // Schema Endpoints (Postgres)
    @Operation(summary = "Create datatable schema", description = "Create a new table definition in Postgres")
    @PostMapping()
    public ResponseEntity<Response> create(@RequestBody Datatable datatable, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "create");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(datatableService.create(datatable, identifier));
    }

    @Operation(summary = "Update datatable schema", description = "Update an existing table definition in Postgres")
    @PutMapping("/{datatableId}")
    public ResponseEntity<Response> update(@PathVariable String datatableId, @RequestBody Datatable datatable,
            @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "update");
        Identifier identifier = Identifier.builder().headers(headers).word(datatableId).build();
        return Response.updateResponse(datatableService.update(datatable, identifier));
    }

    @Operation(summary = "Get datatables by application ID with advanced search")
    @PostMapping("/application/{applicationId}/list")
    public ResponseEntity<Response> getByApplicationId(
            @Parameter(description = "Application ID") @PathVariable String applicationId,
            @Valid @RequestBody(required = false) SearchPayload searchPayload,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String search,
            @RequestParam(required = false, defaultValue = "0") int page,
            @RequestParam(required = false, defaultValue = "10") int size,
            @RequestParam(required = false, defaultValue = "created_at") String sortBy,
            @RequestParam(required = false, defaultValue = "DESC") Sort.Direction direction,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting datatables by application: {}, status: {}, search: {}", applicationId, status, search);

        Pageable pageable = PageRequest.of(page, size, Sort.by(direction, sortBy));
        Identifier identifier = Identifier.builder().headers(headers).pageable(pageable).build();

        StringBuilder searchQuery = new StringBuilder(" AND application_id = '" + applicationId + "'");
        if (status != null && !status.isEmpty()) {
            searchQuery.append(" AND status = '").append(status).append("'");
        }
        if (search != null && !search.isEmpty()) {
            searchQuery.append(" AND (table_name ILIKE '%").append(search).append("%' OR description ILIKE '%")
                    .append(search).append("%')");
        }
        if (!ObjectUtils.isEmpty(searchPayload) && !CollectionUtils.isEmpty(searchPayload.getSearchCriterias())) {
            searchQuery.append(getSearchQuery(searchPayload.getSearchCriterias()));
        }
        identifier.setWord(searchQuery.toString());

        return Response.getResponse(datatableService.list(identifier));
    }

    @Operation(summary = "Get datatables by application", description = "Get all table schemas for an application from Postgres")
    @GetMapping("/application/{applicationId}")
    public ResponseEntity<Response> getAll(@PathVariable String applicationId, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getAll");
        Identifier identifier = Identifier.builder().headers(headers).word(applicationId).build();
        return Response.getResponse(datatableService.get(identifier));
    }

    @Operation(summary = "Get application statistics", description = "Get total tables, records, and storage usage")
    @GetMapping("/application/{applicationId}/stats")
    public ResponseEntity<Response> getStats(@PathVariable String applicationId) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getStats");
        return Response.getResponse(datatableService.getApplicationStats(applicationId));
    }

    // Record Endpoints (MongoDB)
    @Operation(summary = "Add record to table", description = "Insert a new document into MongoDB for the specified table")
    @PostMapping("/{tableId}/record")
    public ResponseEntity<Response> addRecord(@PathVariable String tableId, @RequestBody DataTableRecord record,
            @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "addRecord");
        record.setTableId(tableId);
        Identifier identifier = Identifier.builder().headers(headers).word(tableId).build();
        return Response.createResponse(datatableService.addRecord(record, identifier));
    }

    @Operation(summary = "Get records from table", description = "Fetch paginated documents from MongoDB")
    @GetMapping("/{tableId}/records")
    public ResponseEntity<Response> getRecords(
            @PathVariable String tableId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(defaultValue = "createdAt") String sortBy,
            @RequestParam(defaultValue = "DESC") Sort.Direction direction,
            @RequestParam(required = false) String search,
            @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getRecords");
        Identifier identifier = Identifier.builder()
                .headers(headers)
                .pageable(PageRequest.of(page, size, Sort.by(direction, sortBy)))
                .word(tableId)
                .search(search)
                .build();
        return Response.getResponse(datatableService.getRecords(tableId, identifier));
    }

    @Operation(summary = "Update record in table")
    @PutMapping("/{tableId}/record/{recordId}")
    public ResponseEntity<Response> updateRecord(@PathVariable String tableId, @PathVariable String recordId,
            @RequestBody DataTableRecord record, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "updateRecord");
        record.setId(recordId);
        record.setTableId(tableId);
        Identifier identifier = Identifier.builder().headers(headers).word(tableId).build();
        return Response.updateResponse(datatableService.updateRecord(record, identifier));
    }

    @Operation(summary = "Delete record from table")
    @DeleteMapping("/{tableId}/record/{recordId}")
    public ResponseEntity<Response> deleteRecord(@PathVariable String tableId, @PathVariable String recordId,
            @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "deleteRecord");
        datatableService.deleteRecord(recordId);
        return Response.deleteResponse(true);
    }

    @DeleteMapping("/{datatableId}")
    public ResponseEntity<Response> deleteById(@PathVariable String datatableId, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "deleteById");
        Identifier identifier = Identifier.builder().headers(headers).word(datatableId).build();
        return Response.deleteResponse(datatableService.delete(identifier));
    }
}
