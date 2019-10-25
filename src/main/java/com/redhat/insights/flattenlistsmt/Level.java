package com.redhat.insights.flattenlistsmt;

import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;

class Level {
    private String field;
    private Struct value;
    private List<String> pathTo = new ArrayList<>();

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
