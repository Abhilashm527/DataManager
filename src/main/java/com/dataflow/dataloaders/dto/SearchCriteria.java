package com.dataflow.dataloaders.dto;

import com.dataflow.dataloaders.enums.FieldAndColumn;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SearchCriteria {

    private FieldAndColumn field;
    private Operator operator;
    private Object value;
    private LogicalOperator logicalOperator = LogicalOperator.AND;

    public enum Operator {
        EQUALS, IN, LIKE, ILIKE, ANY, OR
    }

    public enum LogicalOperator {
        AND, OR
    }
}

