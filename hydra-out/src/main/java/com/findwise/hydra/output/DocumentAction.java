package com.findwise.hydra.output;

import java.util.List;

import com.findwise.hydra.local.LocalDocument;


/**
 *
 * @author Jonas Lindmark
 */
public class DocumentAction {
    public enum types {

        list_add, list_delete
    };
    private List<LocalDocument> removelist;
    private List<LocalDocument> addlist;
    private types type;

    public DocumentAction(types type, List<LocalDocument> list) {
        if (type.equals(types.list_add)) {
            this.addlist = list;
        }
        else if (type.equals(types.list_delete)){
            this.removelist = list;
        }
        this.type = type;
    }

    public List<LocalDocument> getAddlist() {
        return addlist;
    }

    public void setAddlist(List<LocalDocument> addlist) {
        this.addlist = addlist;
    }

    public List<LocalDocument> getRemovelist() {
        return removelist;
    }

    public void setRemovelist(List<LocalDocument> removelist) {
        this.removelist = removelist;
    }

    public types getType() {
        return type;
    }

    public void setType(types type) {
        this.type = type;
    }
  
}
