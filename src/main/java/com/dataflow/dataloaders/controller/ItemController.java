package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Item;
import com.dataflow.dataloaders.services.ItemService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static com.dataflow.dataloaders.config.APIConstants.ITEM_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(ITEM_BASE_PATH)
@Tag(name = "Items", description = "Item/Folder management APIs")
public class ItemController {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private ItemService itemService;

    @Operation(summary = "Create item", description = "Create a new item/folder")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Item created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid input")
    })
    @PostMapping
    public ResponseEntity<Response> create(@RequestBody Item folder, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(itemService.create(folder, identifier));
    }

    @Operation(summary = "Get item by ID", description = "Get item/folder by ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Item found"),
            @ApiResponse(responseCode = "404", description = "Item not found")
    })
    @GetMapping("/{itemId}")
    public ResponseEntity<Response> get(@Parameter(description = "Item ID") @PathVariable String itemId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.getResponse(itemService.getFolder(identifier));
    }

    @Operation(summary = "Get root items", description = "Get root level items/folders")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Root items retrieved")
    })
    @GetMapping()
    public ResponseEntity<Response> getRoot(@Parameter(description = "Root filter") @RequestParam(required = false) Boolean root,
                                            @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(itemService.getRoot(identifier));
    }
    
    @Operation(summary = "Delete item", description = "Delete item and all its children")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Item deleted successfully"),
            @ApiResponse(responseCode = "404", description = "Item not found")
    })
    @DeleteMapping("/{itemId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Item ID") @PathVariable String itemId, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.deleteResponse(itemService.deleteAllChildrenUnderId(identifier));
    }

    @Operation(summary = "Update item", description = "Update an existing item")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Item updated successfully"),
            @ApiResponse(responseCode = "404", description = "Item not found")
    })
    @PutMapping("/{itemId}")
    public ResponseEntity<Response> update(@Parameter(description = "Item ID") @PathVariable String itemId, @RequestBody Item request,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.updateResponse(itemService.updateItem(request, identifier));
    }
}

