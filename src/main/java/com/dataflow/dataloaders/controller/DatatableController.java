package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Datatable;
import com.dataflow.dataloaders.entity.Item;
import com.dataflow.dataloaders.services.DatatableService;
import com.dataflow.dataloaders.services.ItemService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/v1/dataloader")
public class DatatableController {
    protected static final String logPrefix = "{} : {}";

    @Autowired
    private DatatableService datatableService;

    @PostMapping("/datatable")
    public ResponseEntity<Response> create(@RequestBody Datatable datatable, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(datatableService.create(datatable, identifier));
    }

    @GetMapping("/datatable/{applicationId}")
    public ResponseEntity<Response> getAll(@PathVariable String applicationId, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(applicationId).build();
        return Response.createResponse(datatableService.get(identifier));
    }
    @DeleteMapping("/datatable/{datatableId}")
    public ResponseEntity<Response> deleteById(@PathVariable String datatableId, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(datatableId).build();
        return Response.createResponse(datatableService.delete(identifier));
    }
}
