package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.DataflowDao;
import com.dataflow.dataloaders.entity.Dataflow;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.stereotype.Service;
import java.util.List;

@Slf4j
@Service
public class DataflowService {

    @Autowired
    private DataflowDao dataflowDao;

    public Dataflow create(Dataflow dataflow, Identifier identifier) {
        log.info("Creating dataflow: {}", dataflow.getDataflowName());
        if (dataflow.getApplicationId() == null || dataflow.getApplicationId().isEmpty()) {
            throw new DataloadersException(ErrorFactory.VALIDATION_ERROR, "Application ID is mandatory");
        }
        return dataflowDao.createV1(dataflow, identifier)
                .orElseThrow(
                        () -> new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to create dataflow"));
    }

    public Dataflow getDataflow(Identifier identifier) {
        log.info("Getting dataflow by id: {}", identifier.getWord());
        return dataflowDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public Page<Dataflow> list(Identifier identifier) {
        log.info("Listing dataflows");
        List<Dataflow> dataflows = dataflowDao.list(identifier);
        long total = dataflows.isEmpty() ? 0 : dataflows.get(0).getTotal();
        return new PageImpl<>(dataflows, identifier.getPageable(), total);
    }

    public Dataflow updateDataflow(Dataflow dataflow, Identifier identifier) {
        log.info("Updating dataflow: {}", identifier.getWord());
        Dataflow existing = dataflowDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        if (dataflow.getDataflowName() != null)
            existing.setDataflowName(dataflow.getDataflowName());
        if (dataflow.getDescription() != null)
            existing.setDescription(dataflow.getDescription());
        if (dataflow.getIsActive() != null)
            existing.setIsActive(dataflow.getIsActive());
        if (dataflow.getIsFavorite() != null)
            existing.setIsFavorite(dataflow.getIsFavorite());
        if (dataflow.getCanvasState() != null)
            existing.setCanvasState(dataflow.getCanvasState());

        existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());
        existing.setUpdatedBy("admin");
        dataflowDao.update(existing);
        return dataflowDao.getV1(identifier).orElse(existing);
    }

    public boolean deleteDataflow(Identifier identifier) {
        log.info("Deleting dataflow: {}", identifier.getWord());
        Dataflow dataflow = dataflowDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
        dataflow.setUpdatedBy("admin");
        return dataflowDao.delete(dataflow) > 0;
    }
}
