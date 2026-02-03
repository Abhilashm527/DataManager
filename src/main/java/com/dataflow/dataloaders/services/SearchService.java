package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.*;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class SearchService {

    @Autowired
    private JobConfigDao jobConfigDao;
    
    @Autowired
    private MappingsDao mappingsDao;
    
    @Autowired
    private ItemDao itemDao;
    
    @Autowired
    private ResourceDao resourceDao;

    public Map<String, Object> globalSearch(String query, Identifier identifier) {
        log.info("Performing global search for: {}", query);
        
        Map<String, Object> results = new HashMap<>();
        
        // Search Applications (Items)
        List<java.util.Map<String, Object>> applications = itemDao.searchByName(query, identifier);
        if (!applications.isEmpty()) {
            results.put("applications", applications);
        }
        
        // Search Resources
        List<java.util.Map<String, Object>> resources = resourceDao.searchByName(query, identifier);
        if (!resources.isEmpty()) {
            results.put("resources", resources);
        }
        
        // Search Job Configs (Dataflows)
        List<java.util.Map<String, Object>> dataflows = jobConfigDao.searchByNameOrDescription(query, identifier);
        if (!dataflows.isEmpty()) {
            results.put("dataflows", dataflows);
        }
        
        // Search Mappings
        List<java.util.Map<String, Object>> mappings = mappingsDao.searchByDescription(query, identifier);
        if (!mappings.isEmpty()) {
            results.put("mappings", mappings);
        }
        
        return results;
    }
}