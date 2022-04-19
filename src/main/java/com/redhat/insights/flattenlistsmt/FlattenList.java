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

import org.apache.commons.lang3.exception.ExceptionUtils;

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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Main project class implementing JSON string transformation.
 */
abstract class FlattenList<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlattenList.class);

    private static final String MODE_ARRAY = "array";
    private static final String MODE_JOIN = "join";
    private static final String MODE_KEYS = "keys";
    private static final String MODE_OBJECT = "object";

    interface ConfigName {
        String SOURCE_FIELD = "sourceField";
        String OUTPUT_FIELD = "outputField";
        String DELIMITER_JOIN = "delimiterJoin";
        String ENCODE = "encode";
        String MODE = "mode";
        String KEYS = "keys";
        String ROOT_KEY = "rootKey";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "Source field name. This field will flattened to output field.")
            .define(ConfigName.OUTPUT_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                            "Output field name. This field will store flattened value of source field.")
            .define(ConfigName.DELIMITER_JOIN, ConfigDef.Type.STRING, "|", ConfigDef.Importance.MEDIUM,
                    "If 'join' mode set, join with that list members into result string.")
            .define(ConfigName.ENCODE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                    "Encode individual segments using URL encoding")
            .define(ConfigName.MODE, ConfigDef.Type.STRING, "array", ConfigDef.Importance.MEDIUM,
                    "How to provide result (array, join, keys).")
            .define(ConfigName.KEYS, ConfigDef.Type.LIST, "", ConfigDef.Importance.MEDIUM,
                    "If 'keys' mode set, list values to these keys.")
            .define(ConfigName.ROOT_KEY, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                    "If 'object' mode set, this defines the key for the root field.");

    private static final String PURPOSE = "flatten source field into the output field";

    private String sourceField;
    private String outputField;
    private String delimiterJoin;
    private boolean encode;
    private String mode;
    private List<String> keys;
    private String rootKey;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        sourceField = config.getString(ConfigName.SOURCE_FIELD);
        outputField = config.getString(ConfigName.OUTPUT_FIELD);
        mode = config.getString(ConfigName.MODE);
        delimiterJoin = config.getString(ConfigName.DELIMITER_JOIN);
        encode = config.getBoolean(ConfigName.ENCODE);
        keys = config.getList(ConfigName.KEYS);
        rootKey = config.getString(ConfigName.ROOT_KEY);

        if (!Collections.unmodifiableList(Arrays.asList(MODE_ARRAY, MODE_JOIN, MODE_KEYS, MODE_OBJECT)).contains(mode)) {
            LOGGER.error("unknown mode '{}'", mode);
            System.exit(1);
        }
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
            Object recordValue = operatingValue(record);
            if (recordValue == null) {
                LOGGER.info("FlattenList record is null");
                LOGGER.info(record.toString());
                return record;
            }

            final Struct value = requireStruct(recordValue, PURPOSE);

            final Schema updatedSchema = makeUpdatedSchema(value, outputField);
            final Struct updatedValue = makeUpdatedValue(value, updatedSchema, sourceField, outputField);

            return newRecord(record, updatedSchema, updatedValue);
        } catch (DataException e) {
            LOGGER.warn("FlattenList fields missing from record.");
            LOGGER.warn(record.toString());
            LOGGER.warn(ExceptionUtils.getStackTrace(e));
            return record;
        }
    }

    private Schema makeUpdatedSchema(Struct value, String outputField) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        for (Field field : value.schema().fields()) {
            builder.field(field.name(), field.schema());
        }

        Schema elementSchema = null;
        switch (mode){
            case MODE_ARRAY:
                elementSchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA);
                break;
            case MODE_JOIN:
                elementSchema = Schema.STRING_SCHEMA;
                break;
            case MODE_KEYS:
                elementSchema = KeysMode.buildElementSchema(keys);
                break;
            case MODE_OBJECT:
                elementSchema = ObjectMode.buildSchema(rootKey, sourceField, value);
                break;
        }
        Schema outFieldSchema = SchemaBuilder.array(elementSchema);
        builder.field(outputField, outFieldSchema);

        return builder.build();
    }

    private Struct makeUpdatedValue(Struct value, Schema updatedSchema, String inputField, String outputField) {
        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field.name()));
        }
        List<List<String>> arr = Processor.expand(value.getStruct(inputField));
        switch (mode){
            case MODE_ARRAY:
                updatedValue.put(outputField, arr);
                break;
            case MODE_JOIN:
                List<String> joinedArr = listOfLists2Joined(arr, delimiterJoin, encode);
                updatedValue.put(outputField, joinedArr);
                break;
            case MODE_KEYS:
                List<Struct> structs = KeysMode.lists2Structs(keys, arr);
                updatedValue.put(outputField, structs);
                break;
            case MODE_OBJECT:
                List<Struct> object = ObjectMode.flattenValue(rootKey, sourceField, value);
                updatedValue.put(outputField, object);
                break;
        }
        return updatedValue;
    }

    private static List<String> listOfLists2Joined(List<List<String>> arr, String delimiterJoin, boolean encode) {
        List<String> joinedArr = new ArrayList<>(arr.size());
        for (List<String> mem : arr) {
            String joined = String.join(delimiterJoin, transformSegments(mem, encode));
            joinedArr.add(joined);
        }
        return joinedArr;
    }

    private static List<String> transformSegments (final List<String> tag, final boolean encode) {
        return tag.stream().map(segment -> {
            if (segment == null) {
                return "";
            }

            if (encode) {
                return encode(segment);
            }

            return segment;
        }).collect(Collectors.toList());
    }

    private static String encode (final String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 not supported", e);
        }
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
