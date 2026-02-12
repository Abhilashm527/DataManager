package com.dataflow.dataloaders.util;

import com.dataflow.dataloaders.dto.SearchCriteria;
import com.dataflow.dataloaders.enums.FieldAndColumn;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryUtil {

    public static String getSearchQuery(List<SearchCriteria> searchCriterias) {
        StringBuilder stringBuilder = new StringBuilder();
        if (searchCriterias == null || searchCriterias.isEmpty()) {
            return stringBuilder.toString();
        }
        
        List<SearchCriteria> searchCriteriasOther = searchCriterias.stream()
                .filter(sc -> !sc.getLogicalOperator().equals(SearchCriteria.LogicalOperator.OR)).toList();
        List<SearchCriteria> searchCriteriasOr = searchCriterias.stream()
                .filter(sc -> sc.getLogicalOperator().equals(SearchCriteria.LogicalOperator.OR)).toList();

        for (SearchCriteria searchCriteria : searchCriteriasOther) {
            switch (searchCriteria.getOperator()) {
                case EQUALS -> {
                    stringBuilder.append(" AND ").append(searchCriteria.getField().getValue()).append(" = ");
                    appendFormattedValue(stringBuilder, searchCriteria.getValue());
                }
                case IN -> {
                    List<?> values = (List<?>) searchCriteria.getValue();
                    String inClause = values.stream()
                            .map(QueryUtil::formatValue)
                            .collect(Collectors.joining(", "));
                    stringBuilder.append(" AND ")
                            .append(searchCriteria.getField().getValue())
                            .append(" IN (").append(inClause).append(")");
                }
                case LIKE -> {
                    stringBuilder.append(" AND ").append(searchCriteria.getField().getValue())
                            .append(" LIKE ")
                            .append(formatValue("%" + searchCriteria.getValue() + "%"));
                }
                case ILIKE -> {
                    stringBuilder.append(" AND ").append(searchCriteria.getField().getValue())
                            .append(" ILIKE ")
                            .append(formatValue("%" + searchCriteria.getValue() + "%"));
                }
                case ANY -> {
                    stringBuilder.append(" AND ")
                            .append(formatValue(searchCriteria.getValue()))
                            .append(" = ANY(")
                            .append(searchCriteria.getField().getValue())
                            .append(")");
                }
            }
        }
        
        if (!searchCriteriasOr.isEmpty()) {
            stringBuilder.append(" OR ");
            String joinedCriteria = searchCriteriasOr.stream()
                    .map(searchCriteria -> searchCriteria.getField().getValue() +
                            " " + searchCriteria.getOperator() + " " +
                            formatValue("%" + searchCriteria.getValue() + "%"))
                    .collect(Collectors.joining(" OR "));
            stringBuilder.append(joinedCriteria).append(" ");
        }
        return stringBuilder.toString();
    }

    private static String formatValue(Object val) {
        if (val instanceof String) {
            return "'" + val + "'";
        }
        return val.toString();
    }

    private static void appendFormattedValue(StringBuilder sb, Object val) {
        if (val instanceof String) {
            sb.append("'").append(val).append("'");
            return;
        }
        sb.append(val);
    }

    public static String getSearchQuery(Set<String> ids, FieldAndColumn fieldAndColumn) {
        String idsAsString = ids.stream().map(Object::toString)
                .collect(Collectors.joining(","));
        return " AND " + fieldAndColumn.getValue() + " IN (" + idsAsString + ")";
    }
}
