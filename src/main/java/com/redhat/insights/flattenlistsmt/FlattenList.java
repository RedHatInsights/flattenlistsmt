/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redhat.insights.flattenlistsmt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Main project class implementing JSON string transformation.
 */
abstract class FlattenList<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlattenList.class);

    interface ConfigName {
        String SOURCE_FIELD = "sourceField";
        String OUTPUT_FIELD = "outputField";
        String DELIMITER_JOIN = "delimiterJoin";
        String JOIN = "join";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "Source field name. This field will flattened to output field.")
            .define(ConfigName.OUTPUT_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                            "Output field name. This field will store flattened value of source field.")
            .define(ConfigName.DELIMITER_JOIN, ConfigDef.Type.STRING, "|", ConfigDef.Importance.MEDIUM,
                    "If join mode set, join with that list members into result string.")
            .define(ConfigName.JOIN, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                    "Join list members to result string.");

    private static final String PURPOSE = "flatten source field into the output field";

    private String sourceField;
    private String outputField;
    private String delimiterJoin;
    private boolean join;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        sourceField = config.getString(ConfigName.SOURCE_FIELD);
        outputField = config.getString(ConfigName.OUTPUT_FIELD);
        delimiterJoin = config.getString(ConfigName.DELIMITER_JOIN);
        join = config.getBoolean(ConfigName.JOIN);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            LOGGER.info("Schemaless records not supported");
            return null;
        } else {
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        try {
            final Struct value = requireStruct(operatingValue(record), PURPOSE);

            final Schema updatedSchema = makeUpdatedSchema(value, outputField);
            final Struct updatedValue = makeUpdatedValue(value, updatedSchema, sourceField, outputField);

            return newRecord(record, updatedSchema, updatedValue);
        } catch (DataException e) {
            LOGGER.warn("FlattenList fields missing from record.");
            LOGGER.warn(record.toString());
            LOGGER.warn(e.toString());
            return record;
        }
    }

    private Schema makeUpdatedSchema(Struct value, String outputField) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        for (Field field : value.schema().fields()) {
            builder.field(field.name(), field.schema());
        }

        Schema outFieldSchema;
        if (join) {
            outFieldSchema = SchemaBuilder.array(Schema.STRING_SCHEMA);
        } else {
            outFieldSchema = SchemaBuilder.array(SchemaBuilder.array(Schema.STRING_SCHEMA)).optional();
        }
        builder.field(outputField, outFieldSchema);

        return builder.build();
    }

    private Struct makeUpdatedValue(Struct value, Schema updatedSchema, String inputField, String outputField) {
        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field.name()));
        }
        List<List<String>> arr = Processor.expand(value.getStruct(inputField));
        if (join) {
            List<String> joinedArr = listOfLists2Joined(arr, delimiterJoin);
            updatedValue.put(outputField, joinedArr);
        } else {
            updatedValue.put(outputField, arr);
        }
        return updatedValue;
    }

    private static List<String> listOfLists2Joined(List<List<String>> arr, String delimiterJoin) {
        List<String> joinedArr = new ArrayList<>(arr.size());
        for (List<String> mem : arr) {
            String joined = String.join(delimiterJoin, mem);
            joinedArr.add(joined);
        }
        return joinedArr;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() { }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Value<R extends ConnectRecord<R>> extends FlattenList<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
