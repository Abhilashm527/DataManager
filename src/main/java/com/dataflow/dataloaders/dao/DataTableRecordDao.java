package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.DataTableRecord;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class DataTableRecordDao {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private IdGenerator idGenerator;

    public DataTableRecord insert(DataTableRecord record, Identifier identifier) {
        if (record.getId() == null) {
            record.setId(idGenerator.generateId());
        }
        if (record.getCreatedAt() == null) {
            record.setCreatedAt(DateUtils.getUnixTimestampInUTC());
        }
        record.setUpdatedAt(DateUtils.getUnixTimestampInUTC());
        populateSearchText(record);
        return mongoTemplate.save(record);
    }

    public List<DataTableRecord> insertAll(List<DataTableRecord> records, Identifier identifier) {
        long now = DateUtils.getUnixTimestampInUTC();
        for (DataTableRecord record : records) {
            if (record.getId() == null) {
                record.setId(idGenerator.generateId());
            }
            record.setCreatedAt(now);
            record.setUpdatedAt(now);
            populateSearchText(record);
        }
        return (List<DataTableRecord>) mongoTemplate.insertAll(records);
    }

    public List<DataTableRecord> findByTableId(String tableId) {
        Query query = new Query(Criteria.where("tableId").is(tableId));
        return mongoTemplate.find(query, DataTableRecord.class);
    }

    public Page<DataTableRecord> findByTableId(String tableId, Identifier identifier) {
        Query query = new Query(Criteria.where("tableId").is(tableId));
        
        if (identifier.getSearch() != null && !identifier.getSearch().isEmpty()) {
            query.addCriteria(Criteria.where("searchText").regex(identifier.getSearch(), "i"));
        }

        long total = mongoTemplate.count(query, DataTableRecord.class);

        if (identifier.getPageable() != null) {
            query.with(identifier.getPageable());
        }

        List<DataTableRecord> records = mongoTemplate.find(query, DataTableRecord.class);
        return new PageImpl<>(records, identifier.getPageable() != null ? identifier.getPageable() : Pageable.unpaged(), total);
    }

    public Optional<DataTableRecord> findById(String recordId) {
        return Optional.ofNullable(mongoTemplate.findById(recordId, DataTableRecord.class));
    }

    public DataTableRecord update(DataTableRecord record, Identifier identifier) {
        record.setUpdatedAt(DateUtils.getUnixTimestampInUTC());
        populateSearchText(record);
        return mongoTemplate.save(record);
    }

    private void populateSearchText(DataTableRecord record) {
        if (record.getData() == null || record.getData().isEmpty()) {
            record.setSearchText("");
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (Object value : record.getData().values()) {
            if (value != null) {
                sb.append(value.toString()).append(" ");
            }
        }
        record.setSearchText(sb.toString().trim());
    }

    public void deleteById(String recordId) {
        mongoTemplate.remove(new Query(Criteria.where("id").is(recordId)), DataTableRecord.class);
    }
    public void createIndex(Index index){
        mongoTemplate.indexOps(DataTableRecord.class).ensureIndex(index);
    }

    public void deleteByTableId(String tableId) {
        mongoTemplate.remove(new Query(Criteria.where("tableId").is(tableId)), DataTableRecord.class);
    }

    public long countByTableId(String tableId) {
        Query query = new Query(Criteria.where("tableId").is(tableId));
        return mongoTemplate.count(query, DataTableRecord.class);
    }

    public List<DataTableRecord> search(String tableId, String searchText) {
        Query query = new Query();
        query.addCriteria(Criteria.where("tableId").is(tableId));
        if (searchText != null && !searchText.isEmpty()) {
            query.addCriteria(Criteria.where("searchText").regex(searchText, "i"));
        }
        return mongoTemplate.find(query, DataTableRecord.class);
    }
}
