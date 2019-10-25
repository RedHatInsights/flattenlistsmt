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

import static org.junit.Assert.*;

public class FlattenListTest {
    private FlattenList<SinkRecord> xform = new FlattenList.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        xform.configure(new HashMap<>());

        final Map<String, Object> value = new HashMap<>();
        value.put("name", "Josef");
        value.put("age", 42);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);
        assertNull(transformedRecord);
    }

    @Test
    public void basicCase() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "tags");
        props.put("outputField", "tags_flat");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.struct()
                        .field("sat", SchemaBuilder.struct()
                                .field("env", SchemaBuilder.array(Schema.STRING_SCHEMA).optional())))
                .build();

        final Struct value = new Struct(schema);
        final Struct tags = new Struct(schema.field("tags").schema());
        final Struct sat = new Struct(tags.schema().field("sat").schema());
        final List<String> env = new ArrayList<>(1);
        env.add("prod");
        sat.put("env", env);
        tags.put("sat", sat);
        value.put("tags", tags);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(2, updatedValue.schema().fields().size());
        assertEquals(1, updatedValue.getArray("tags_flat").size());
        System.out.println(updatedValue.toString());
    }

    @Test
    public void multipleTags() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "tags");
        props.put("outputField", "tags_flat");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.struct()
                        .field("sat", SchemaBuilder.struct()
                                .field("env", SchemaBuilder.array(Schema.STRING_SCHEMA).optional())))
                .build();

        final Struct value = new Struct(schema);
        final Struct tags = new Struct(schema.field("tags").schema());
        final Struct sat = new Struct(tags.schema().field("sat").schema());
        final List<String> env = new ArrayList<>(1);
        env.add("test1");
        env.add("test2");
        env.add("prod");
        sat.put("env", env);
        tags.put("sat", sat);
        value.put("tags", tags);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();
        assertEquals(2, updatedValue.schema().fields().size());
        assertEquals(3, updatedValue.getArray("tags_flat").size());
        System.out.println(updatedValue.toString());
    }
}
