package com.redhat.insights.flattenlistsmt;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);

    static List<List<String>> expand(Struct struct) {
        List<List<String>> result = new ArrayList<>();
        if (struct == null) {
            return result;
        }

        Level root = new Level();
        root.setValue(struct);
        List<Level> links = expand(root);
        for (Level link : links) {
            result.add(link.getPathTo());
        }
        return result;
    }

    private static List<Level> expand(Level level) {
        List<Level> outLevels = new ArrayList<>();
        for (Field field : level.getValue().schema().fields()) {
            Level childLevel = new Level();
            childLevel.getPathTo().addAll(level.getPathTo());
            childLevel.getPathTo().add(field.name());
            Schema.Type valueSchemaType = field.schema().type();

            switch (valueSchemaType) {
                case STRUCT:
                    childLevel.setField(field.name());
                    childLevel.setValue(level.getValue().getStruct(field.name()));
                    List<Level> childLinks = expand(childLevel);
                    outLevels.addAll(childLinks);
                    break;

                case STRING:
                    childLevel.getPathTo().add(level.getValue().getString(field.name()));
                    break;

                case ARRAY:
                    Schema.Type elementSchemaType = field.schema().valueSchema().type();
                    if (field.schema().valueSchema().type() != Schema.Type.STRUCT) {
                        for (Object obj : level.getValue().getArray(field.name())) {
                            Level elementLevel = new Level();
                            elementLevel.getPathTo().addAll(childLevel.getPathTo());
                            String val = obj == null ? null : String.format("%s", obj);
                            elementLevel.getPathTo().add(val);
                            outLevels.add(elementLevel);
                        }
                    } else {
                        LOGGER.warn(String.format("Array member schema '%s' not supported",
                                elementSchemaType.toString()));
                    }
                    break;

                default:
                    LOGGER.warn(String.format("Schema '%s' not supported", valueSchemaType.toString()));
            }
        }
        return outLevels;
    }
}
