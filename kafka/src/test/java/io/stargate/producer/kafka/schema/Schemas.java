package io.stargate.producer.kafka.schema;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class Schemas {
  // todo this will be replaced by schema registry calls when the integration with schema registry
  // will be implemented
  public static final String SCHEMA_NAMESPACE = "io.stargate.producer.kafka";
  public static final String KEY_RECORD_NAME = "clusterName.keyspace.table.Key";
  public static final String VALUE_RECORD_NAME = "clusterName.keyspace.table.Value";
  public static final String PARTITION_KEY_NAME = "pk_1";

  public static final String COLUMN_NAME = "col_1";

  public static final String CLUSTERING_KEY_NAME = "ck_1";

  // all PKs and Clustering Keys are required (non-optional)
  public static final Schema KEY_SCHEMA =
      SchemaBuilder.record(KEY_RECORD_NAME)
          .namespace(SCHEMA_NAMESPACE)
          .fields()
          .requiredString(PARTITION_KEY_NAME)
          .requiredInt(CLUSTERING_KEY_NAME)
          .endRecord();

  public static final Schema VALUE_SCHEMA;

  static {
    Schema timestampMillisType =
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));

    Schema partitionKey =
        SchemaBuilder.record(PARTITION_KEY_NAME).fields().requiredString("value").endRecord();
    Schema partitionKeyNullable =
        SchemaBuilder.unionOf().type(partitionKey).and().nullType().endUnion();

    Schema clusteringKey =
        SchemaBuilder.record(CLUSTERING_KEY_NAME).fields().requiredInt("value").endRecord();
    Schema clusteringKeyNullable =
        SchemaBuilder.unionOf().type(clusteringKey).and().nullType().endUnion();

    Schema column = SchemaBuilder.record(COLUMN_NAME).fields().optionalString("value").endRecord();
    Schema columnNullable = SchemaBuilder.unionOf().type(column).and().nullType().endUnion();

    Schema fields =
        SchemaBuilder.record("columns")
            .fields()
            .name(PARTITION_KEY_NAME)
            .type(partitionKeyNullable)
            .noDefault()
            .name(CLUSTERING_KEY_NAME)
            .type(clusteringKeyNullable)
            .noDefault()
            .name(COLUMN_NAME)
            .type(columnNullable)
            .noDefault()
            .endRecord();

    VALUE_SCHEMA =
        SchemaBuilder.record(VALUE_RECORD_NAME)
            .namespace(SCHEMA_NAMESPACE)
            .fields()
            .requiredString("op")
            .name("ts_ms")
            .type(timestampMillisType)
            .noDefault()
            .name("data")
            .type(fields)
            .noDefault()
            .endRecord();
  }
}
