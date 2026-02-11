package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.dto.ProviderRequest;
import com.dataflow.dataloaders.entity.Provider;
import com.dataflow.dataloaders.services.ProviderService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.web.multipart.MultipartFile;

import static com.dataflow.dataloaders.config.APIConstants.PROVIDERS_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(PROVIDERS_BASE_PATH)
@Tag(name = "Providers", description = "Provider management APIs")
public class ProviderController {

    @Autowired
    private ProviderService providerService;

    @Operation(summary = "Create provider with optional icon upload")
    @PostMapping()
    public ResponseEntity<Response> create(
            @RequestBody Provider provider,
            @RequestHeader HttpHeaders headers) {
        log.info("Creating provider: {}}", provider.getProviderName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(providerService.create(provider, identifier));
    }

    @Operation(summary = "Get provider by ID")
    @GetMapping("/{providerId}")
    public ResponseEntity<Response> get(@Parameter(description = "Provider ID") @PathVariable Long providerId,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting provider: {}", providerId);
        Identifier identifier = Identifier.builder().headers(headers).id(providerId).build();
        return Response.getResponse(providerService.getProvider(identifier));
    }

    @Operation(summary = "Get all providers")
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info("Getting all providers");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(providerService.getAllProviders(identifier));
    }

    @Operation(summary = "Get providers by connection type")
    @GetMapping("/connection-type/{connectionTypeId}")
    public ResponseEntity<Response> getByConnectionType(
            @Parameter(description = "Connection Type ID") @PathVariable String connectionTypeId,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting providers by connection type: {}", connectionTypeId);
        return Response.getResponse(providerService.getProvidersByConnectionType(connectionTypeId));
    }

    @Operation(summary = "Update provider")
    @PutMapping("/{providerId}")
    public ResponseEntity<Response> update(@Parameter(description = "Provider ID") @PathVariable Long providerId,
            @RequestBody Provider provider,
            @RequestHeader HttpHeaders headers) {
        log.info("Updating provider: {}", providerId);
        Identifier identifier = Identifier.builder().headers(headers).id(providerId).build();
        return Response.updateResponse(providerService.updateProvider(provider, identifier));
    }

    @Operation(summary = "Delete provider")
    @DeleteMapping("/{providerId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Provider ID") @PathVariable Long providerId,
            @RequestHeader HttpHeaders headers) {
        log.info("Deleting provider: {}", providerId);
        Identifier identifier = Identifier.builder().headers(headers).id(providerId).build();
        return Response.deleteResponse(providerService.deleteProvider(identifier));
    }
}
