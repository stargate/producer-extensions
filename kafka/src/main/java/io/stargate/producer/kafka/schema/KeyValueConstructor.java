package io.stargate.producer.kafka.schema;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.stargate.db.CellValue;
import org.apache.cassandra.stargate.db.RowMutationEvent;
import org.jetbrains.annotations.NotNull;

public class KeyValueConstructor {

  private SchemaProvider schemaProvider;
  public static final String OPERATION_FIELD_NAME = "op";
  public static final String TIMESTAMP_FIELD_NAME = "ts_ms";
  public static final String DATA_FIELD_NAME = "data";

  public KeyValueConstructor(SchemaProvider schemaProvider) {
    this.schemaProvider = schemaProvider;
  }

  @NotNull
  public GenericRecord constructValue(RowMutationEvent mutationEvent, String topicName) {
    Schema schema = schemaProvider.getValueSchemaForTopic(topicName);
    System.out.println("schema: " + schema);
    GenericRecord value = new GenericData.Record(schema);

    Schema dataSchema =
        schema.getFields().stream()
            .filter(f -> f.name().equals(DATA_FIELD_NAME))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "There is no %s field in the record schema for topic: %s.",
                            DATA_FIELD_NAME, topicName)))
            .schema();

    GenericRecord data = new Record(dataSchema);

    createUnionAndAppendToData(mutationEvent.getPartitionKeys(), dataSchema, data);
    createUnionAndAppendToData(mutationEvent.getClusteringKeys(), dataSchema, data);
    createUnionAndAppendToData(mutationEvent.getCells(), dataSchema, data);

    value.put(OPERATION_FIELD_NAME, OperationType.UPDATE.getAlias());
    value.put(TIMESTAMP_FIELD_NAME, mutationEvent.getTimestamp());
    value.put(DATA_FIELD_NAME, data);

    return value;
  }

  /** All Partition Keys and Clustering Keys must be included in the kafka.key */
  @NotNull
  public GenericRecord constructKey(RowMutationEvent mutationEvent, String topicName) {
    GenericRecord key = new GenericData.Record(schemaProvider.getKeySchemaForTopic(topicName));

    fillGenericRecordWithData(mutationEvent.getPartitionKeys(), key);
    fillGenericRecordWithData(mutationEvent.getClusteringKeys(), key);

    return key;
  }

  private void createUnionAndAppendToData(
      List<? extends CellValue> cellValues, Schema dataSchema, GenericRecord data) {
    cellValues.forEach(
        pk -> {
          String columnName = pk.getColumn().getName();
          Schema unionSchema = dataSchema.getField(columnName).schema();
          if (!unionSchema.getType().equals(Type.UNION)) {
            throw new IllegalStateException(
                String.format(
                    "The type for %s should be UNION but is: %s",
                    columnName, unionSchema.getType()));
          }
          GenericRecord record =
              new Record(
                  unionSchema.getTypes().get(1)); // 0 - is null type, 1 - is an actual union type
          record.put("value", pk.getValueObject());
          data.put(columnName, record);
        });
  }

  private void fillGenericRecordWithData(
      List<? extends CellValue> cellValues, GenericRecord genericRecord) {
    cellValues.forEach(
        cellValue -> {
          genericRecord.put(cellValue.getColumn().getName(), cellValue.getValueObject());
        });
  }
}
