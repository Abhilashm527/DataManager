package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Dataflow;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.DFUtil;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class DataflowDao extends GenericDaoImpl<Dataflow, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private DFUtil dfUtil;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Optional<Dataflow> createV1(Dataflow model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            String id = insertDataflow(model, identifier);
            return getV1(Identifier.builder().word(id).build());
        } catch (Exception e) {
            log.error("Error creating dataflow: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    public String insertDataflow(Dataflow model, Identifier identifier) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("Dataflow.create"), new String[] { "id" });
            ps.setString(1, model.getId());
            ps.setObject(2, model.getApplicationId());
            ps.setString(3, model.getDataflowName());
            ps.setString(4, model.getDescription());
            ps.setObject(5, model.getIsActive() != null ? model.getIsActive() : true);
            ps.setObject(6, model.getIsFavorite() != null ? model.getIsFavorite() : false);
            ps.setString(7, model.getCanvasState() != null ? model.getCanvasState().toString() : null);
            ps.setObject(8, model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            ps.setObject(9, DateUtils.getUnixTimestampInUTC());
            return ps;
        });
        return model.getId();
    }

    @Override
    public Optional<Dataflow> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Dataflow.getById"), dataflowRowMapper, identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<Dataflow> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Dataflow.list", identifier), dataflowRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public int update(Dataflow dataflow) {
        try {
            return jdbcTemplate.update(getSql("Dataflow.updateById"),
                    dataflow.getDataflowName(),
                    dataflow.getDescription(),
                    dataflow.getIsActive(),
                    dataflow.getIsFavorite(),
                    dataflow.getCanvasState() != null ? dataflow.getCanvasState().toString() : null,
                    dataflow.getUpdatedBy() != null ? dataflow.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    dataflow.getId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public int delete(Dataflow dataflow) {
        try {
            return jdbcTemplate.update(getSql("Dataflow.deleteById"),
                    dataflow.getUpdatedBy() != null ? dataflow.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    dataflow.getId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    private final RowMapper<Dataflow> dataflowRowMapper = (rs, rowNum) -> {
        Dataflow dataflow = new Dataflow();
        dataflow.setId(rs.getString("id"));
        dataflow.setApplicationId(rs.getString("application_id"));
        dataflow.setDataflowName(rs.getString("dataflow_name"));
        dataflow.setDescription(rs.getString("description"));
        dataflow.setIsActive(rs.getObject("is_active") != null ? rs.getBoolean("is_active") : null);
        dataflow.setIsFavorite(rs.getObject("is_favorite") != null ? rs.getBoolean("is_favorite") : null);
        dataflow.setCanvasState(dfUtil.readValueToJsonNode(rs.getString("canvas_state")));
        dataflow.setCreatedAt(rs.getObject("created_at", Long.class));
        dataflow.setCreatedBy(rs.getString("created_by"));
        dataflow.setUpdatedAt(rs.getObject("updated_at", Long.class));
        dataflow.setUpdatedBy(rs.getString("updated_by"));
        dataflow.setDeletedAt(rs.getObject("deleted_at", Long.class));
        dataflow.setTotal(rs.getObject("total") != null ? rs.getLong("total") : null);
        return dataflow;
    };

    @Override
    public Long insert(Dataflow model, Identifier identifier) {
        return 0L;
    }

    @Override
    public Optional<Dataflow> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<Dataflow> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<Dataflow> updateV1(Dataflow transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Dataflow> hotUpdate(Dataflow transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public String setInvalues(String query, String replaceString, Set<String> inValues) {
        return super.setInvalues(query, replaceString, inValues);
    }
}
