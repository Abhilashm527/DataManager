package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.services.SearchService;
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

@Slf4j
@RestController
@RequestMapping("/api/search")
@Tag(name = "Search", description = "Global search APIs")
public class SearchController {

    @Autowired
    private SearchService searchService;

    @Operation(summary = "Global search", description = "Search across all entities")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Search results retrieved"),
            @ApiResponse(responseCode = "400", description = "Query too short")
    })
    @GetMapping
    public ResponseEntity<Response> globalSearch(@Parameter(description = "Search query (min 3 characters)") @RequestParam String query,
                                               @RequestHeader HttpHeaders headers) {
        log.info("Global search with query: {}", query);
        
        if (query.length() < 3) {
            return Response.createResponse("Query must be at least 3 characters");
        }
        
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(searchService.globalSearch(query, identifier));
    }
}