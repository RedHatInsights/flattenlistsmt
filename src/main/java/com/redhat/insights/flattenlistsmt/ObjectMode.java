package com.redhat.insights.flattenlistsmt;

        import org.apache.kafka.connect.data.Field;
        import org.apache.kafka.connect.data.Schema;
        import org.apache.kafka.connect.data.SchemaBuilder;
        import org.apache.kafka.connect.data.Struct;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import java.util.ArrayList;
        import java.util.List;

class ObjectMode {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeysMode.class);

    static Schema buildSchema(final String rootKey, final Struct value) {
        SchemaBuilder structBuilder = SchemaBuilder.struct();
        structBuilder.field(rootKey, Schema.OPTIONAL_STRING_SCHEMA);

        //build the transformed schema from the first field. This assumes all fields have the same children.
        //if the child fields do not match, the transformation of the values will fail
        List<Field> childFields = value.schema().fields().get(0).schema().fields().get(0).schema().fields();

        for (Field childField : childFields) {
            SchemaBuilder childBuilder = new SchemaBuilder(childField.schema().type());
            childBuilder.optional();
            structBuilder.field(childField.name(), childBuilder.build());
        }
        return structBuilder.build();
    }

    static List<Struct> flattenValue(final String rootKey, final Struct value) {
        Schema updatedSchema = buildSchema(rootKey, value);
        List<Struct> flatObjects = new ArrayList<>();

        Struct unwrappedValue = (Struct) value.get(value.schema().fields().get(0));
        for (Field rootField : unwrappedValue.schema().fields()) {
            Struct flatObject = new Struct(updatedSchema);
            flatObject.put(rootKey, rootField.name());

            Struct rootValue = (Struct) unwrappedValue.get(rootField.name());
            for (Field childField : rootField.schema().fields()) {
                flatObject.put(childField.name(), rootValue.get(childField.name()));
            }
            flatObjects.add(flatObject);
        }
        return flatObjects;
    }
}
