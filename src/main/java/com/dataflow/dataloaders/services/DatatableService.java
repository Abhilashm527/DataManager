package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.DatatableDao;
import com.dataflow.dataloaders.entity.Datatable;
import com.dataflow.dataloaders.util.Identifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatatableService {

    @Autowired
    private DatatableDao datatableDao;

    public Object create(Datatable datatable, Identifier identifier) {
        datatableDao.create(datatable, identifier);
        return true;
    }

    public Object get(Identifier identifier) {
        List<Datatable> datatables = datatableDao.getByApplicationId(identifier);
        return datatables;
    }

    public Object delete(Identifier identifier) {
        datatableDao.deleteByDatatableId(identifier);
        return true;
    }
}
