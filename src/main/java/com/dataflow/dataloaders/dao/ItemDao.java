package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Item;
import com.dataflow.dataloaders.entity.ItemType;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Repository
public class ItemDao extends GenericDaoImpl<Item, Identifier, String> {
    public static final String ITEM_ID = "id";

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Item create(@NotNull Item model, Identifier identifier) {
        return super.create(model, identifier);
    }

    @Override
    public Optional<Item> createV1(Item model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            String itemId = insertItem(model, identifier);
            return getV1(new Identifier(itemId));
        } catch (DataIntegrityViolationException e) {
            handleDataIntegrityViolationException(e);
        } catch (Exception e) {
            handleGenericException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    public String insertItem(Item model, Identifier identifier) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("Item.create"));
            ps.setObject(1, model.getId());
            ps.setObject(2, model.getParentId());
            ps.setObject(3, model.getParentFolderId());
            ps.setObject(4, model.getName());
            ps.setObject(5, model.getType());
            ps.setObject(6, model.getItemType() != null ? model.getItemType().getValue() : null);
            ps.setObject(7, model.getPath());
            ps.setObject(8, model.getItemReference());
            ps.setObject(9, model.getDeletable());
            ps.setObject(10, model.getActive());
            ps.setObject(11, model.getRoot());
            ps.setObject(12, model.getVersion());
            ps.setObject(13, model.getCreatedBy());
            ps.setObject(14, DateUtils.getUnixTimestampInUTC());
            return ps;
        });
        return model.getId();
    }

    @Override
    public Long insert(Item model, Identifier identifier) {
        return 0L;
    }

    @Override
    public Item upsert(Item model, Identifier identifier) {
        return super.upsert(model, identifier);
    }

    @Override
    public Item upsert(Item model, Identifier identifier, String whereClause) {
        return super.upsert(model, identifier, whereClause);
    }

    @Override
    public Item get(@NotNull Identifier identifier) {
        return super.get(identifier);
    }

    @Override
    public Optional<Item> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(getSql("Item.getById"), itemRowMapper,
                    identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public Item get(Identifier identifier, String whereClause) {
        return super.get(identifier, whereClause);
    }

    @Override
    public Optional<Item> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    public int updateName(Item item, Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("Item.UpdateById"),
                    item.getName(),
                    item.getParentId(),
                    item.getRoot(),
                    item.getUpdatedBy(),
                    DateUtils.getUnixTimestampInUTC(),
                    item.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public List<Item> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Item.getRootFolders"), itemRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public Optional<Item> findByReferenceId(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(getSql("Item.findByReferencedId"), itemRowMapper,
                    identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public int deleteByReferenceId(Item item, Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("Item.DeleteByReferenceId"),
                    item.getUpdatedBy(),
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    item.getItemReference());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public List<Item> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Item update(Item transientObject, Identifier identifier) {
        return super.update(transientObject, identifier);
    }

    @Override
    public Optional<Item> updateV1(Item transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Item> hotUpdate(Item transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Item item) {
        try {
            return jdbcTemplate.update(getSql("Item.DeleteById"),
                    item.getUpdatedBy(),
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    item.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int deleteV1(Optional<Item> persistentObject) {
        return super.deleteV1(persistentObject);
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public int findAndDelete(Identifier identifier) {
        return super.findAndDelete(identifier);
    }

    @Override
    public <E extends Number> String setInvalues(String query, String replaceString, Set<E> inValues, String... delimitter) {
        return super.setInvalues(query, replaceString, inValues, delimitter);
    }

    @Override
    public String setInvalues(String query, String replaceString, Set<String> inValues) {
        return super.setInvalues(query, replaceString, inValues);
    }

    public List<Item> getChildrenByParentId(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Item.getChildrenByParentId"), itemRowMapper, identifier.getWord());
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    private void handleDataIntegrityViolationException(DataIntegrityViolationException e) {
        String errorMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : e.getMessage();
        if (errorMessage != null) {
            if (errorMessage.contains(ITEM_ID)) {
                throw new DataloadersException(ErrorFactory.DUPLICATION, "An Item with this ID already exists");
            }
        }
        throw new DataloadersException(ErrorFactory.DUPLICATION, "An Item with this data already exists");
    }

    private void handleGenericException(Exception e) {
        logger.error("Exception Occurred: {}", e.getMessage());
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
    }

    public List<java.util.Map<String, Object>> searchByName(String query, Identifier identifier) {
        try {
            String sql = "SELECT id, name, type, item_type FROM items " +
                        "WHERE LOWER(name) LIKE LOWER(?) " +
                        "AND deleted_at IS NULL ORDER BY name";
            String searchPattern = "%" + query + "%";
            return jdbcTemplate.queryForList(sql, searchPattern);
        } catch (Exception e) {
            logger.error("Error searching items: {}", e.getMessage());
            return List.of();
        }
    }

    RowMapper<Item> itemRowMapper = (rs, rowNum) -> {
        Item item = new Item();

        item.setId(rs.getString("id"));
        item.setParentId(rs.getString("parent_id"));
        item.setParentFolderId(rs.getString("parent_folder_id"));
        item.setName(rs.getString("name"));
        item.setType(rs.getString("type"));
        String type = rs.getString("item_type");
        item.setItemType(type != null ? ItemType.fromString(type) : null);
        item.setPath(rs.getString("path"));
        item.setItemReference(rs.getString("item_reference"));
        item.setDeletable(rs.getObject("deletable") != null && rs.getBoolean("deletable"));
        item.setActive(rs.getObject("active") != null && rs.getBoolean("active"));
        item.setRoot(rs.getObject("root") != null && rs.getBoolean("root"));
        item.setVersion(rs.getString("version"));
        item.setCreatedBy(rs.getString("created_by"));
        item.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        item.setUpdatedBy(rs.getString("updated_by"));
        item.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);

        return item;
    };
}