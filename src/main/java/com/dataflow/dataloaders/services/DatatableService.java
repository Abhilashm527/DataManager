package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.DatatableDao;
import com.dataflow.dataloaders.dao.DataTableRecordDao;
import com.dataflow.dataloaders.entity.Datatable;
import com.dataflow.dataloaders.entity.DataTableRecord;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.PartialIndexFilter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class DatatableService {

    @Autowired
    private DatatableDao datatableDao;

    @Autowired
    private DataTableRecordDao recordDao;

    // Schema Management (Postgres)
    public Datatable create(Datatable datatable, Identifier identifier) {
        validateSchema(datatable);
        Datatable created = datatableDao.createV1(datatable, identifier).orElse(null);
        createIndexes(created);
        if (created != null)
            populateStats(created);
        return created;
    }

    public Datatable update(Datatable datatable, Identifier identifier) {
        datatableDao.updateDatatable(datatable, identifier);
        return datatableDao.getV1(new Identifier(datatable.getId())).orElse(null);
    }

    public Datatable getById(Identifier identifier) {
        Datatable table = datatableDao.getV1(identifier).orElse(null);
        if (table != null)
            populateStats(table);
        return table;
    }

    public List<Datatable> get(Identifier identifier) {
        List<Datatable> tables = datatableDao.getByApplicationId(identifier);
        tables.forEach(this::populateStats);
        return tables;
    }

    public Page<Datatable> list(Identifier identifier) {
        List<Datatable> tables = datatableDao.list(identifier);
        long totalElements = tables.isEmpty() ? 0
                : (tables.get(0).getTotal() != null ? tables.get(0).getTotal() : tables.size());
        tables.forEach(this::populateStats);
        return new org.springframework.data.domain.PageImpl<>(tables, identifier.getPageable(), totalElements);
    }

    public List<Datatable> search(String applicationId, String search) {
        List<Datatable> tables = datatableDao.searchInApplication(applicationId, search);
        tables.forEach(this::populateStats);
        return tables;
    }

    public boolean delete(Identifier identifier) {
        datatableDao.deleteByDatatableId(identifier);
        recordDao.deleteByTableId(identifier.getWord());
        return true;
    }

    // Record Management (MongoDB)
    public DataTableRecord addRecord(DataTableRecord record, Identifier identifier) {
        return recordDao.insert(record, identifier);
    }

    public List<DataTableRecord> addRecords(List<DataTableRecord> records, Identifier identifier) {
        return recordDao.insertAll(records, identifier);
    }

    public Page<DataTableRecord> getRecords(String tableId, Identifier identifier) {
        return recordDao.findByTableId(tableId, identifier);
    }

    public DataTableRecord updateRecord(DataTableRecord record, Identifier identifier) {
        return recordDao.update(record, identifier);
    }

    public void deleteRecord(String recordId) {
        recordDao.deleteById(recordId);
    }

    // Statistics & AI Features
    public Map<String, Object> getApplicationStats(String applicationId) {
        List<Datatable> tables = datatableDao.getByApplicationId(new Identifier(applicationId));
        long totalTables = tables.size();
        long totalRecords = 0;

        for (Datatable table : tables) {
            totalRecords += recordDao.countByTableId(table.getId());
        }

        Map<String, Object> stats = new HashMap<>();
        stats.put("totalTables", totalTables);
        stats.put("totalRecords", totalRecords);
        stats.put("applicationId", applicationId);
        // Assuming some mock storage usage calculation
        stats.put("storageUsedBytes", totalRecords * 1024);
        return stats;
    }

    private void populateStats(Datatable table) {
        if (table != null) {
            try {
                long rCount = recordDao.countByTableId(table.getId());
                int fCount = 0;
                if (table.getColumns() != null) {
                    fCount = table.getColumns().size();
                } else {
                    log.warn("Columns list is null for table: {}", table.getTableName());
                }
                table.setRecordCount(rCount);
                table.setFieldCount(fCount);
                log.info("Populated stats for {}: records={}, fields={}", table.getTableName(), rCount, fCount);
            } catch (Exception e) {
                log.error("Error populating stats for datatable: {}", table.getTableName(), e);
                table.setRecordCount(0L);
                table.setFieldCount(0);
            }
        }
    }
    private void validateSchema(Datatable schema) {
        if (schema.getTableName() == null || schema.getTableName().trim().isEmpty()) {
            throw new RuntimeException("Table name is required");
        }

        if (schema.getColumns() == null || schema.getColumns().isEmpty()) {
            throw new RuntimeException("At least one column is required");
        }

        // Check for duplicate column names
        Set<String> columnNames = new HashSet<>();
        for (Datatable.ColumnDefinition column : schema.getColumns()) {
            if (column.getName() == null || column.getName().trim().isEmpty()) {
                throw new RuntimeException("Column name is required");
            }

            if (columnNames.contains(column.getName())) {
                throw new RuntimeException("Duplicate column name: " + column.getName());
            }

            columnNames.add(column.getName());

            if (column.getDataType() == null || column.getDataType().trim().isEmpty()) {
                throw new RuntimeException("Data type is required for column: " + column.getName());
            }
        }
    }
    public void createIndexes(Datatable schema) {
        if (schema.getColumns() == null)
            return;

        for (Datatable.ColumnDefinition column : schema.getColumns()) {
            if (column.isIndexed() || column.isUnique()) {
                // Index 'data.columnName'
                String fieldPath = "data." + column.getName();
                // Name convention: idx_TABLEID_COLUMN
                String indexName = "idx_" + schema.getId() + "_" + column.getName();

                Index index = new Index().on(fieldPath, Sort.Direction.ASC).named(indexName);

                if (column.isUnique()) {
                    index.unique();
                }

                // Partial Filter: { 'tableId': schemaId }
                // This ensures the index only applies to records belonging to THIS table
                index.partial(PartialIndexFilter.of(Criteria.where("tableId").is(schema.getId())));

                try {
                    recordDao.createIndex(index);
                } catch (Exception e) {
                    // In production, log this.
                    System.err.println("Error creating index " + indexName + ": " + e.getMessage());
                }
            }
        }
    }

}
