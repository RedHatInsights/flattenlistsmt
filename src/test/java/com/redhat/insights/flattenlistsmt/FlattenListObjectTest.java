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
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FlattenListObjectTest {
    private final FlattenList<SinkRecord> xform = new FlattenList.Value<>();

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @After
    public void restoreStreams() {
        System.setOut(originalOut);
        System.setErr(originalErr);
        xform.close();
    }

    @Test
    public void basicCase() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "per_reporter_staleness");
        props.put("outputField", "per_reporter_staleness_flat");
        props.put("mode", "object");
        props.put("rootKey", "reporter");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("per_reporter_staleness", SchemaBuilder.struct()
                        .field("FOO", SchemaBuilder.struct()
                                .field("last_check_in", SchemaBuilder.STRING_SCHEMA)
                                .field("stale_timestamp", SchemaBuilder.STRING_SCHEMA)
                                .field("check_in_succeeded", SchemaBuilder.BOOLEAN_SCHEMA)))
                .build();

        final Struct value = new Struct(schema);
        final Struct perReporterStaleness = new Struct(schema.field("per_reporter_staleness").schema());

        final Struct foo = new Struct(perReporterStaleness.schema().field("FOO").schema());
        foo.put("last_check_in", "2022-04-14T15:00:09.785661+00:00");
        foo.put("stale_timestamp","2022-04-15T15:00:09.583189+00:00");
        foo.put("check_in_succeeded", true);
        perReporterStaleness.put("FOO", foo);

        value.put("per_reporter_staleness", perReporterStaleness);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        String expected = "Struct{" +
                "per_reporter_staleness=Struct{" +
                    "FOO=Struct{last_check_in=2022-04-14T15:00:09.785661+00:00,stale_timestamp=2022-04-15T15:00:09.583189+00:00,check_in_succeeded=true}}," +
                "per_reporter_staleness_flat=[" +
                    "Struct{reporter=FOO,last_check_in=2022-04-14T15:00:09.785661+00:00,stale_timestamp=2022-04-15T15:00:09.583189+00:00,check_in_succeeded=true}]}";

        assertEquals(expected, updatedValue.toString());
    }

    @Test
    public void multipleReporters() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "per_reporter_staleness");
        props.put("outputField", "per_reporter_staleness_flat");
        props.put("mode", "object");
        props.put("rootKey", "reporter");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("per_reporter_staleness", SchemaBuilder.struct()
                    .field("FOO", SchemaBuilder.struct()
                        .field("last_check_in", SchemaBuilder.STRING_SCHEMA)
                        .field("stale_timestamp", SchemaBuilder.STRING_SCHEMA)
                        .field("check_in_succeeded", SchemaBuilder.BOOLEAN_SCHEMA))
                    .field("BAR", SchemaBuilder.struct()
                            .field("last_check_in", SchemaBuilder.STRING_SCHEMA)
                            .field("stale_timestamp", SchemaBuilder.STRING_SCHEMA)
                            .field("check_in_succeeded", SchemaBuilder.BOOLEAN_SCHEMA)))
                .build();

        final Struct value = new Struct(schema);
        final Struct perReporterStaleness = new Struct(schema.field("per_reporter_staleness").schema());

        final Struct foo = new Struct(perReporterStaleness.schema().field("FOO").schema());
        foo.put("last_check_in", "2022-04-14T15:00:09.785661+00:00");
        foo.put("stale_timestamp","2022-04-15T15:00:09.583189+00:00");
        foo.put("check_in_succeeded", true);
        perReporterStaleness.put("FOO", foo);

        final Struct bar = new Struct(perReporterStaleness.schema().field("BAR").schema());
        bar.put("last_check_in", "2021-04-14T15:00:09.785661+00:00");
        bar.put("stale_timestamp","2021-04-15T15:00:09.583189+00:00");
        bar.put("check_in_succeeded", false);
        perReporterStaleness.put("BAR", bar);

        value.put("per_reporter_staleness", perReporterStaleness);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        String expected = "Struct{" +
                "per_reporter_staleness=Struct{" +
                    "FOO=Struct{last_check_in=2022-04-14T15:00:09.785661+00:00,stale_timestamp=2022-04-15T15:00:09.583189+00:00,check_in_succeeded=true}," +
                    "BAR=Struct{last_check_in=2021-04-14T15:00:09.785661+00:00,stale_timestamp=2021-04-15T15:00:09.583189+00:00,check_in_succeeded=false}}," +
                "per_reporter_staleness_flat=[" +
                    "Struct{reporter=FOO,last_check_in=2022-04-14T15:00:09.785661+00:00,stale_timestamp=2022-04-15T15:00:09.583189+00:00,check_in_succeeded=true}, " +
                    "Struct{reporter=BAR,last_check_in=2021-04-14T15:00:09.785661+00:00,stale_timestamp=2021-04-15T15:00:09.583189+00:00,check_in_succeeded=false}]}";

        assertEquals(expected, updatedValue.toString());
    }

    @Test
    public void missingKey() {
        final Map<String, String> props = new HashMap<>();
        props.put("sourceField", "per_reporter_staleness");
        props.put("outputField", "per_reporter_staleness_flat");
        props.put("mode", "object");
        props.put("rootKey", "reporter");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("per_reporter_staleness", SchemaBuilder.struct()
                        .field("FOO", SchemaBuilder.struct()
                                .field("last_check_in", SchemaBuilder.STRING_SCHEMA)
                                .field("stale_timestamp", SchemaBuilder.STRING_SCHEMA)
                                .field("check_in_succeeded", SchemaBuilder.BOOLEAN_SCHEMA))
                        .field("BAR", SchemaBuilder.struct() //Missing last_check_in
                                .field("stale_timestamp", SchemaBuilder.STRING_SCHEMA)
                                .field("check_in_succeeded", SchemaBuilder.BOOLEAN_SCHEMA)))
                .build();

        final Struct value = new Struct(schema);
        final Struct perReporterStaleness = new Struct(schema.field("per_reporter_staleness").schema());

        final Struct foo = new Struct(perReporterStaleness.schema().field("FOO").schema());
        foo.put("last_check_in", "2022-04-14T15:00:09.785661+00:00");
        foo.put("stale_timestamp","2022-04-15T15:00:09.583189+00:00");
        foo.put("check_in_succeeded", true);
        perReporterStaleness.put("FOO", foo);

        final Struct bar = new Struct(perReporterStaleness.schema().field("BAR").schema());
        bar.put("stale_timestamp","2021-04-15T15:00:09.583189+00:00");
        bar.put("check_in_succeeded", false);
        perReporterStaleness.put("BAR", bar);

        value.put("per_reporter_staleness", perReporterStaleness);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        String expected = "Struct{" +
                "per_reporter_staleness=Struct{" +
                "FOO=Struct{last_check_in=2022-04-14T15:00:09.785661+00:00,stale_timestamp=2022-04-15T15:00:09.583189+00:00,check_in_succeeded=true}," +
                "BAR=Struct{stale_timestamp=2021-04-15T15:00:09.583189+00:00,check_in_succeeded=false}}," +
                "per_reporter_staleness_flat=[" +
                "Struct{reporter=FOO,last_check_in=2022-04-14T15:00:09.785661+00:00,stale_timestamp=2022-04-15T15:00:09.583189+00:00,check_in_succeeded=true}, " +
                "Struct{reporter=BAR,stale_timestamp=2021-04-15T15:00:09.583189+00:00,check_in_succeeded=false}]}";

        assertEquals(expected, updatedValue.toString());
    }
}
