package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Item;
import com.dataflow.dataloaders.services.ItemService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static com.dataflow.dataloaders.config.APIConstants.ITEM_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(ITEM_BASE_PATH)
public class ItemController {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private ItemService itemService;

    @PostMapping
    public ResponseEntity<Response> create(@RequestBody Item folder, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(itemService.create(folder, identifier));
    }

    @GetMapping("/{itemId}")
    public ResponseEntity<Response> get(@PathVariable String itemId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.getResponse(itemService.getFolder(identifier));
    }

    @GetMapping()
    public ResponseEntity<Response> getRoot(@RequestParam(required = false) Boolean root,
                                            @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(itemService.getRoot(identifier));
    }
    @DeleteMapping("/{itemId}")
    public ResponseEntity<Response> delete(@PathVariable String itemId, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.deleteResponse(itemService.deleteAllChildrenUnderId(identifier));
    }

    @PutMapping("/{itemId}")
    public ResponseEntity<Response> update(@PathVariable String itemId, @RequestBody Item request,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.updateResponse(itemService.updateItem(request, identifier));
    }
}

