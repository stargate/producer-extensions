package io.stargate.producer.kafka.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class Schemas {
  public static final String PARTITION_KEY_NAME = "pk_1";

  public static final String COLUMN_NAME = "col_1";

  public static final String CLUSTERING_KEY_NAME = "ck_1";

  // all PKs and Clustering Keys are required (non-optional)
  public static final Schema KEY_SCHEMA =
      SchemaBuilder.record("key")
          .fields()
          .requiredString(PARTITION_KEY_NAME)
          .requiredInt(CLUSTERING_KEY_NAME)
          .endRecord();

  public static final Schema VALUE_SCHEMA =
      SchemaBuilder.record("value").fields().requiredString(COLUMN_NAME).endRecord();
}
