package com.dataflow.dataloaders.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class SearchPayload {

    private List<SearchCriteria> searchCriterias;
}
