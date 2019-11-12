package com.redhat.insights.flattenlistsmt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class KeysMode {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeysMode.class);

    static Schema buildElementSchema(final List<String> keys) {
        SchemaBuilder structBuilder = SchemaBuilder.struct();
        for (String key : keys) {
            structBuilder.field(key, Schema.STRING_SCHEMA);
        }
        Schema elementSchema = structBuilder.build();
        return elementSchema;
    }

    static List<Struct> lists2Structs(final List<String> keys, final List<List<String>> lists) {
        Schema elementSchema = buildElementSchema(keys);
        List<Struct> resultList = new ArrayList<>(lists.size());
        for (List<String> list : lists) {
            final Struct struct = list2Struct(keys, list, elementSchema);
            resultList.add(struct);

        }
        return resultList;
    }

    private static Struct list2Struct(final List<String> keys, final List<String> list, final Schema elementSchema) {
        Struct struct = new Struct(elementSchema);
        if (keys.size() != list.size()) {
            LOGGER.warn("Keys count ({}) not equal to values count ({}), returning empty struct", keys.size(),
                    list.size());
            return struct;
        }

        for (int i = 0; i < keys.size(); i++) {
            struct.put(keys.get(i), list.get(i));
        }
        return struct;
    }
}
