package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.dto.DataflowCanvasDto;
import com.dataflow.dataloaders.entity.Dataflow;
import com.dataflow.dataloaders.services.DataflowService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static com.dataflow.dataloaders.config.APIConstants.BASE_PATH;

@Slf4j
@RestController
@RequestMapping(BASE_PATH + "/dataflow-canvas")
@Tag(name = "Dataflow Canvas", description = "Dataflow Canvas positioning APIs")
public class DataflowCanvasController {

    @Autowired
    private DataflowService dataflowService;

    @Operation(summary = "Save dataflow canvas state")
    @PostMapping
    public ResponseEntity<Response> saveCanvas(@RequestBody DataflowCanvasDto canvasDto,
            @RequestHeader HttpHeaders headers) {
        log.info("Saving canvas state for dataflow: {}", canvasDto.getWorkflow().getDataflowId());
        String dataflowId = canvasDto.getWorkflow().getDataflowId();
        Identifier identifier = Identifier.builder().headers(headers).word(dataflowId).build();

        Dataflow update = new Dataflow();
        update.setId(dataflowId);
        update.setCanvasState(canvasDto.getCanvasState());

        dataflowService.updateDataflow(update, identifier);
        return Response.updateResponse("Canvas state saved successfully");
    }

    @Operation(summary = "Get dataflow canvas state")
    @GetMapping("/{dataflowId}")
    public ResponseEntity<Response> getCanvas(@PathVariable String dataflowId, @RequestHeader HttpHeaders headers) {
        log.info("Getting canvas state for dataflow: {}", dataflowId);
        Identifier identifier = Identifier.builder().headers(headers).word(dataflowId).build();
        Dataflow dataflow = dataflowService.getDataflow(identifier);

        DataflowCanvasDto responseDto = DataflowCanvasDto.builder()
                .workflow(DataflowCanvasDto.WorkflowInfoDto.builder()
                        .dataflowId(dataflow.getId())
                        .name(dataflow.getDataflowName())
                        .updatedAt(dataflow.getUpdatedAtDisplay())
                        .build())
                .canvasState(dataflow.getCanvasState())
                .build();

        return Response.getResponse(responseDto);
    }
}
