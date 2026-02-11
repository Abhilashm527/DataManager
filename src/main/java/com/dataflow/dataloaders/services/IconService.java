package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.IconDao;
import com.dataflow.dataloaders.entity.Icon;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.AbstractHandlerMethodAdapter;

import java.io.IOException;

import java.util.List;
import java.util.Optional;

@Slf4j
@Service
public class IconService {

    @Autowired
    private IconDao iconDao;
    @Autowired
    private AbstractHandlerMethodAdapter abstractHandlerMethodAdapter;

    public Icon create(Icon icon, Identifier identifier) {
        log.info("Creating icon: {}", icon.getIconName());
        return iconDao.create(icon, identifier);
    }

    public Icon getIcon(Identifier identifier) {

        log.info("Getting icon by id: {}", identifier.getId());
        return iconDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public List<Icon> getAllIcons(Identifier identifier) {
        log.info("Getting all icons");
        List<Icon> iconList = iconDao.list(identifier);
        if(iconList.isEmpty()){
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }
        return iconList;
    }

    public List<Icon> getAllByModule(Identifier identifier) {
        log.info("Getting all icons");
        List<Icon> iconList = iconDao.listByModule(identifier);
        if(iconList.isEmpty()){
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }
        return iconList;
    }

    public Icon updateIcon(Icon icon, Identifier identifier) {
        log.info("Updating icon: {}", identifier.getId());
        Icon existing = iconDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        if (icon.getIconName() != null)
            existing.setIconName(icon.getIconName());
        if (icon.getModule() != null)
            existing.setModule(icon.getModule());
        if (icon.getIcon() != null)
            existing.setIcon(icon.getIcon());
        existing.setUpdatedBy("admin");
        existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        iconDao.update(existing);
        return iconDao.getV1(identifier).orElse(existing);
    }

    public boolean deleteIcon(Identifier identifier) {
        log.info("Deleting icon: {}", identifier.getId());
        Icon icon = iconDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
        icon.setUpdatedBy("admin");
        return iconDao.delete(icon) > 0;
    }
}
