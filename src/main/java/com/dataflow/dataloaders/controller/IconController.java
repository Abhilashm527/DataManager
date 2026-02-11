package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Icon;
import com.dataflow.dataloaders.services.IconService;
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
import static com.dataflow.dataloaders.config.APIConstants.ICONS_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(ICONS_BASE_PATH)
@Tag(name = "Icons", description = "Icon management APIs")
public class IconController {

    @Autowired
    private IconService iconService;

    @Operation(summary = "Create icon")
    @PostMapping
    public ResponseEntity<Response> create(@RequestBody Icon icon, @RequestHeader HttpHeaders headers) {
        log.info("Creating icon");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(iconService.create(icon, identifier));
    }

    @Operation(summary = "Get icon by ID")
    @GetMapping("/{iconId}")
    public ResponseEntity<Response> get(@Parameter(description = "Icon ID") @PathVariable Long iconId,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting icon: {}", iconId);
        Identifier identifier = Identifier.builder().headers(headers).id(iconId).build();
        return Response.getResponse(iconService.getIcon(identifier));
    }

    @Operation(summary = "Get all icons")
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info("Getting all icons");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(iconService.getAllIcons(identifier));
    }

    @Operation(summary = "Update icon")
    @PutMapping("/{iconId}")
    public ResponseEntity<Response> update(@Parameter(description = "Icon ID") @PathVariable Long iconId,
            @RequestBody Icon icon,
            @RequestHeader HttpHeaders headers) {
        log.info("Updating icon: {}", iconId);
        Identifier identifier = Identifier.builder().headers(headers).id(iconId).build();
        return Response.updateResponse(iconService.updateIcon(icon, identifier));
    }

    @Operation(summary = "Delete icon")
    @DeleteMapping("/{iconId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Icon ID") @PathVariable Long iconId,
            @RequestHeader HttpHeaders headers) {
        log.info("Deleting icon: {}", iconId);
        Identifier identifier = Identifier.builder().headers(headers).id(iconId).build();
        return Response.deleteResponse(iconService.deleteIcon(identifier));
    }

    @Operation(summary = "Get icon image by ID")
    @GetMapping("/{iconId}/image")
    public ResponseEntity<byte[]> getIconImage(
            @Parameter(description = "Icon ID") @PathVariable Long iconId,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting icon image: {}", iconId);
        Identifier identifier = Identifier.builder().headers(headers).id(iconId).build();
        Icon icon = iconService.getIcon(identifier);

        if (icon.getIconData() == null) {
            log.warn("Icon {} has no binary data", iconId);
            return ResponseEntity.notFound().build();
        }

        MediaType contentType = MediaType.IMAGE_PNG; // default
        if (icon.getContentType() != null) {
            try {
                contentType = MediaType.parseMediaType(icon.getContentType());
            } catch (Exception e) {
                log.warn("Invalid content type: {}, using default", icon.getContentType());
            }
        }

        return ResponseEntity.ok()
                .contentType(contentType)
                .body(icon.getIconData());
    }
}
