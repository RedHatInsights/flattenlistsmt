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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FlattenListKeysTest {
    private FlattenList<SinkRecord> xform = new FlattenList.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void basicCase() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "tags");
        props.put("outputField", "tags_flat");
        props.put("mode", "keys");
        props.put("keys", "namespace,key,value");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.struct()
                        .field("Sat", SchemaBuilder.struct()
                                .field("env", SchemaBuilder.array(Schema.STRING_SCHEMA).optional())))
                .build();

        final Struct value = new Struct(schema);
        final Struct tags = new Struct(schema.field("tags").schema());
        final Struct sat = new Struct(tags.schema().field("Sat").schema());
        final List<String> env = new ArrayList<>(1);
        env.add("prod");
        sat.put("env", env);
        tags.put("Sat", sat);
        value.put("tags", tags);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        String expected = "Struct{tags=Struct{Sat=Struct{env=[prod]}}," +
                                 "tags_flat=[Struct{namespace=Sat,key=env,value=prod}]}";
        assertEquals(expected, updatedValue.toString());
    }

    @Test
    public void multipleTags() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "tags");
        props.put("outputField", "tags_flat");
        props.put("mode", "keys");
        props.put("keys", "namespace,key,value");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.struct()
                        .field("Sat", SchemaBuilder.struct()
                                .field("env", SchemaBuilder.array(Schema.STRING_SCHEMA).optional())))
                .build();

        final Struct value = new Struct(schema);
        final Struct tags = new Struct(schema.field("tags").schema());
        final Struct sat = new Struct(tags.schema().field("Sat").schema());
        final List<String> env = new ArrayList<>(1);
        env.add("test1");
        env.add("test2");
        env.add("prod");
        sat.put("env", env);
        tags.put("Sat", sat);
        value.put("tags", tags);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        String expected = "Struct{tags=Struct{Sat=Struct{env=[test1, test2, prod]}}," +
                                 "tags_flat=[Struct{namespace=Sat,key=env,value=test1}, " +
                                            "Struct{namespace=Sat,key=env,value=test2}, " +
                                            "Struct{namespace=Sat,key=env,value=prod}]}";
        assertEquals(expected, updatedValue.toString());
    }

    @Test
    public void nullLeaf() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "tags");
        props.put("outputField", "tags_flat");
        props.put("mode", "keys");
        props.put("keys", "namespace,key,value");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.struct()
                        .field("Sat", SchemaBuilder.struct()
                                .field("env", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional())))
                .build();

        final Struct value = new Struct(schema);
        final Struct tags = new Struct(schema.field("tags").schema());
        final Struct sat = new Struct(tags.schema().field("Sat").schema());
        final List<String> env = new ArrayList<>(1);
        env.add("prod");
        env.add(null);
        sat.put("env", env);
        tags.put("Sat", sat);
        value.put("tags", tags);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        String expected = "Struct{tags=Struct{Sat=Struct{env=[prod, null]}}," +
                                 "tags_flat=[Struct{namespace=Sat,key=env,value=prod}, " +
                                            "Struct{namespace=Sat,key=env,value=null}]}";
        assertEquals(expected, updatedValue.toString());
    }

    @Test
    public void multipleHighLevels() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "tags");
        props.put("outputField", "tags_flat");
        props.put("mode", "keys");
        props.put("keys", "namespace,key,value");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.struct()
                        .field("Sat", SchemaBuilder.struct()
                                .field("env", SchemaBuilder.array(Schema.STRING_SCHEMA).optional()))
                        .field("client", SchemaBuilder.struct()
                                .field("group", SchemaBuilder.array(Schema.STRING_SCHEMA).optional())))
                .build();

        final Struct value = new Struct(schema);
        final Struct tags = new Struct(schema.field("tags").schema());
        final Struct sat = new Struct(tags.schema().field("Sat").schema());
        final List<String> env = new ArrayList<>(1);
        env.add("prod");
        sat.put("env", env);
        tags.put("Sat", sat);
        final Struct client = new Struct(tags.schema().field("client").schema());
        final List<String> group = new ArrayList<>(1);
        group.add("foo");
        group.add("bar");
        client.put("group", group);
        tags.put("client", client);
        value.put("tags", tags);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        String expected = "Struct{tags=Struct{Sat=Struct{env=[prod]},client=Struct{group=[foo, bar]}}," +
                                 "tags_flat=[Struct{namespace=Sat,key=env,value=prod}, " +
                                            "Struct{namespace=client,key=group,value=foo}, " +
                                            "Struct{namespace=client,key=group,value=bar}]}";
        assertEquals(expected, updatedValue.toString());
    }

    /**
     * Null value is converted to empty list.
     */
    @Test
    public void nullValue() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "tags");
        props.put("outputField", "tags_flat");
        props.put("mode", "keys");
        props.put("keys", "namespace,key,value");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.struct()
                        .field("Sat", SchemaBuilder.struct()
                                .field("env", SchemaBuilder.array(Schema.STRING_SCHEMA)
                                        .optional())).optional())
                .build();

        final Struct value = new Struct(schema);
        value.put("tags", null);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        String expected = "Struct{tags_flat=[]}";
        assertEquals(expected, updatedValue.toString());
    }

    /**
     * Unexpected schema, on error it returns original value.
     */
    @Test
    public void intValue() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "tags");
        props.put("outputField", "tags_flat");
        props.put("mode", "keys");
        props.put("keys", "namespace,key,value");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("tags", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("tags", 1);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        String expected = "Struct{tags=1}";
        assertEquals(expected, updatedValue.toString());
    }

    @Test
    public void keysLowSize() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "tags");
        props.put("outputField", "tags_flat");
        props.put("mode", "keys");
        props.put("keys", "namespace,key");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.struct()
                        .field("Sat", SchemaBuilder.struct()
                                .field("env", SchemaBuilder.array(Schema.STRING_SCHEMA).optional())))
                .build();

        final Struct value = new Struct(schema);
        final Struct tags = new Struct(schema.field("tags").schema());
        final Struct sat = new Struct(tags.schema().field("Sat").schema());
        final List<String> env = new ArrayList<>(1);
        env.add("prod");
        sat.put("env", env);
        tags.put("Sat", sat);
        value.put("tags", tags);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        String expected = "Struct{tags=Struct{Sat=Struct{env=[prod]}}}";
        assertEquals(expected, updatedValue.toString());
    }

    @Test
    public void keysHighSize() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "tags");
        props.put("outputField", "tags_flat");
        props.put("mode", "keys");
        props.put("keys", "namespace,key,level1,level2");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.struct()
                        .field("Sat", SchemaBuilder.struct()
                                .field("env", SchemaBuilder.array(Schema.STRING_SCHEMA).optional())))
                .build();

        final Struct value = new Struct(schema);
        final Struct tags = new Struct(schema.field("tags").schema());
        final Struct sat = new Struct(tags.schema().field("Sat").schema());
        final List<String> env = new ArrayList<>(1);
        env.add("prod");
        sat.put("env", env);
        tags.put("Sat", sat);
        value.put("tags", tags);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        String expected = "Struct{tags=Struct{Sat=Struct{env=[prod]}}}";
        assertEquals(expected, updatedValue.toString());
    }
}
