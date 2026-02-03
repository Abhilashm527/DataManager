package com.dataflow.dataloaders.dao;


import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;


@Component
public abstract class GenericDaoImpl<T, I, S> implements GenericDao<T, I, S> {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    protected Environment environment;
    protected String tableName = "<table_name>";
    protected String rowCreationDate = "row_creation_date";
    protected String rowCreatedBy = "row_created_by";
    protected String rowUpdatedBy = "row_updated_by";
    protected String rowUpdateInfo = "row_update_info";
    protected static final String UIDS = "%UIDS%";
    protected static final String CDS = "%CDS%";
    protected static final String CLASSES = "%CLASSES%";
    protected String primaryKeyErr = "Not able to generate PK";

    protected String getSql(String key) {
        String sql = this.environment.getProperty(key);
        if (sql != null) {
            return sql;
        } else {
            throw new DataloadersException("The requested SQL query is not found");
        }
    }

    protected String getSql(String key, @NotNull Identifier identifier) {
        String baseSql = this.environment.getProperty(key);
        if (baseSql != null && !baseSql.isBlank()) {
            return StringUtils.isEmpty(identifier.getWord()) ? baseSql + this.paginatedSQL(identifier.getPageable()) : baseSql + identifier.getWord() + this.paginatedSQL(identifier.getPageable());
        } else {
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND, "The requested SQL query is not found for key: " + key);
        }
    }

    protected String formatSearchText(String data) {
        return "%" + data + "%";
    }

    protected String paginatedSQL(Pageable pageable) {
        Sort sort = pageable.getSort();
        StringBuilder orderByClause = new StringBuilder();
        if (sort.isSorted()) {
            orderByClause.append(" ORDER BY ");
            sort.forEach((order) -> orderByClause.append(toSnakeCase(order.getProperty())).append(" ").append(order.getDirection().name()).append(", "));
            orderByClause.setLength(orderByClause.length() - 2);
        }

        int limit = pageable.getPageSize();
        int offset = pageable.getPageNumber() * limit;
        String paginationClause = " LIMIT " + limit + " OFFSET " + offset;
        String var10000 = String.valueOf(orderByClause);
        return var10000 + paginationClause;
    }

    protected static String toSnakeCase(String input) {
        return input != null && !input.isEmpty() ? input.replaceAll("([a-z])([A-Z]+)", "$1_$2").replaceAll("([A-Z])([A-Z][a-z])", "$1_$2").toLowerCase() : input;
    }
}

