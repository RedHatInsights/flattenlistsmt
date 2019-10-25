package com.redhat.insights.flattenlistsmt;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;

class TreeLink {
    private String field;
    private Struct value;
    private List<String> pathTo = new ArrayList<>();

    static List<TreeLink> expand(TreeLink treeLink) {
        List<TreeLink> outTreeLinks = new ArrayList<>();
        if (treeLink.getValue() == null) {
            return outTreeLinks;
        }

        for (Field field : treeLink.value.schema().fields()) {
            TreeLink childTreeLink = new TreeLink();
            childTreeLink.getPathTo().addAll(treeLink.getPathTo());
            childTreeLink.getPathTo().add(field.name());
            Schema.Type valueSchemaType = treeLink.getValue().schema().field(field.name()).schema().type();
            if (valueSchemaType == Schema.Type.STRUCT) {
                childTreeLink.setField(field.name());
                childTreeLink.setValue(treeLink.getValue().getStruct(field.name()));
                List<TreeLink> childLinks = expand(childTreeLink);
                outTreeLinks.addAll(childLinks);
            } else if (valueSchemaType == Schema.Type.STRING) {
                childTreeLink.getPathTo().add(treeLink.getValue().getString(field.name()));
            } else if (valueSchemaType == Schema.Type.ARRAY) {
                if (field.schema().valueSchema().type() != Schema.Type.STRUCT) {
                    for (Object obj : treeLink.getValue().getArray(field.name())) {
                        TreeLink arrMemTreeLink = new TreeLink();
                        arrMemTreeLink.getPathTo().addAll(childTreeLink.getPathTo());
                        arrMemTreeLink.getPathTo().add(obj.toString());
                        outTreeLinks.add(arrMemTreeLink);
                    }
                }
            } else {
                outTreeLinks.add(childTreeLink);
            }
        }
        return outTreeLinks;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Struct getValue() {
        return value;
    }

    public void setValue(Struct value) {
        this.value = value;
    }

    public List<String> getPathTo() {
        return pathTo;
    }

    public void setPathTo(List<String> pathTo) {
        this.pathTo = pathTo;
    }
}
