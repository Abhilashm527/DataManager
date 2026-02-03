package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.services.SearchService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/search")
public class SearchController {

    @Autowired
    private SearchService searchService;

    @GetMapping
    public ResponseEntity<Response> globalSearch(@RequestParam String query,
                                               @RequestHeader HttpHeaders headers) {
        log.info("Global search with query: {}", query);
        
        if (query.length() < 3) {
            return Response.createResponse("Query must be at least 3 characters");
        }
        
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(searchService.globalSearch(query, identifier));
    }
}