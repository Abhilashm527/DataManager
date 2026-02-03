package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ItemDao;
import com.dataflow.dataloaders.entity.Item;
import com.dataflow.dataloaders.entity.ItemType;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
public class ItemService {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private ItemDao itemDao;

    public Item create(Item item, Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "create - identifier: {}", identifier);
        Item builtItem = buildFolderRequest(new Item(), item);
        Item created = itemDao.create(builtItem, identifier);
        log.info("Created item: {}", created);

        // If it's an APPLICATION, create the 3 default subfolders
        if (created.getItemType() == ItemType.APPLICATIONS) {
            createDefaultSubfolders(created, identifier);
        }

        return created;
    }

    private void createDefaultSubfolders(Item parentItem, Identifier identifier) {
        ItemType[] subfolderTypes = {
                ItemType.RESOURCES,
                ItemType.DATAFLOWS,
                ItemType.MAPPINGS,
//                ItemType.API_ENDPOINT,
//                ItemType.APPLICATIONS,
//                ItemType.AZURE_CONFIGS,
//                ItemType.SECURITY,
//                ItemType.SCHEDULER,
                ItemType.DATATABLES
        };

        for (ItemType subfolderType : subfolderTypes) {
            Item subItem = Item.builder()
                    .name(subfolderType.getValue())  // "Resources", "Dataflows", "Mappings"
                    .itemType(subfolderType)
                    .parentId(parentItem.getId())
                    .parentFolderId(parentItem.getId())
                    .deletable(false)
                    .active(true)
                    .root(false)
                    .version("0.0.1")
                    .build();

            itemDao.create(subItem, identifier);
            log.info("Created subfolder: {} under parent: {}", subfolderType.getValue(), parentItem.getId());
        }
    }

    public Item getFolder(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getFolder - identifier: {}", identifier);
        Item item = itemDao.getV1(identifier)
                .orElseThrow(() -> {
                    log.warn("Folder not found for identifier: {}", identifier);
                    return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                });
        // Set children with subfolder icon
        List<Item> children = itemDao.getChildrenByParentId(new Identifier(item.getId()));
        if (!children.isEmpty()) {
            item.setChildren(children);
            log.info("Found {} children for folder: {}", children.size(), item.getId());
        }
        return item;
    }

    public List<Item> getRoot(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getRoot - identifier: {}", identifier);
        List<Item> items = itemDao.list(identifier);
        if (items.isEmpty()) {
            log.warn("No root items found for identifier: {}", identifier);
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }
        log.info("Found {} root items", items.size());
        return items;
    }
    public boolean deleteItemByReferenceId(Item item, Identifier identifier) {
        log.info("deleteItemByReferenceId - identifier: {}, itemReference: {}", identifier, item.getItemReference());
        identifier.setWord(item.getItemReference());
        Optional<Item> item1 = findItemByReferenceId(identifier);
        boolean deleted = itemDao.deleteByReferenceId(item1.get(), identifier) > 0;
        log.info("Deleted item by reference ID: {}, success: {}", item1.get().getId(), deleted);
        return deleted;
    }

    public Optional<Item> findItemByReferenceId(Identifier identifier) {
        log.info("findItemByReferenceId - identifier: {}", identifier);
        Optional<Item> optionalItem = itemDao.findByReferenceId(identifier);
        if (optionalItem.isEmpty()) {
            log.warn("Item not found by referenceId: {}", identifier);
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }
        return optionalItem;
    }

    public Object deleteAllChildrenUnderId(Identifier identifier) {
        log.info("deleteAllChildrenUnderId - identifier: {}", identifier);
        Item item = getFolder(identifier);
        deleteRecursively(item);
        itemDao.delete(item);
        log.info("Deleted root item and all children under identifier: {}", identifier);
        return true;
    }

    private void deleteRecursively(Item item) {
        if (item.getChildren() == null || item.getChildren().isEmpty()) {
            log.debug("No children to delete for item: {}", item.getId());
            return;
        }
        for (Item child : item.getChildren()) {
            Identifier childIdentifier = new Identifier();
            childIdentifier.setWord(child.getId());
            log.info("Recursively deleting child item: {}", child.getId());
            Item childItem = getFolder(childIdentifier);
            deleteRecursively(childItem);
            itemDao.delete(childItem);
            log.info("Deleted child item: {}", child.getId());
        }
    }

    public Item updateItem(Item item, Identifier identifier) {
        log.info("updateItem - identifier: {}, item: {}", identifier, item);
        Optional<Item> folder = itemDao.getV1(identifier);
        if (folder.isEmpty()) {
            log.warn("Item not found for update - identifier: {}", identifier);
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }
        Item item1 = folder.get();
        item1.setRoot(item.getParentId() == null);
        item1.setName(item.getName() == null ? item1.getName() : item.getName());
        item1.setParentId(item.getParentId() == null ? item1.getParentId() : item.getParentId());
        item1.setUpdatedBy("admin");
        boolean updated = itemDao.updateName(item1, identifier) > 0;
        if (!updated) {
            log.error("Failed to update item: {}", item1.getId());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Not updated");
        }
        log.info("Updated item: {}", item1.getId());
        return getFolder(identifier);
    }

    private Item buildFolderRequest(Item item, Item Item) {
        log.debug("Building item from Item: {}", Item);
        item.setName(Item.getName() != null ? Item.getName() : item.getName());
        item.setParentId(Item.getParentId() != null ? Item.getParentId() : item.getParentId());
        item.setType(Item.getType() != null ? Item.getType() : item.getType());
        item.setItemType(Item.getItemType() != null ? Item.getItemType() : item.getItemType());
        item.setItemReference(Item.getItemReference() != null ? Item.getItemReference() : item.getItemReference());
        item.setPath(Item.getPath() != null ? Item.getPath() : item.getPath());
        item.setDeletable(Item.getDeletable() != null ? Item.getDeletable() : item.getDeletable());
        item.setActive(Item.getActive() != null ? Item.getActive() : item.getActive());
        item.setRoot(item.getParentId() == null);
        item.setVersion(Item.getVersion() != null ? Item.getVersion() : item.getVersion());
        return item;
    }
}
