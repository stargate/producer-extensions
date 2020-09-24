package io.stargate.producer.kafka;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;

import com.google.common.io.Resources;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import static io.stargate.producer.kafka.schema.Schemas.CLUSTERING_KEY_NAME;
import static io.stargate.producer.kafka.schema.Schemas.COLUMN_NAME;
import static io.stargate.producer.kafka.schema.Schemas.PARTITION_KEY_NAME;
import static io.stargate.producer.kafka.schema.Schemas.SCHEMA_NAMESPACE;

public class AvroTest {
	@Test
	public void shouldCreateSchema() throws IOException {

		Schema timestampMillisType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
		Schema fields = SchemaBuilder.record("data")
				.fields().optionalString(PARTITION_KEY_NAME).endRecord();

		Schema partitionKey = SchemaBuilder.record(PARTITION_KEY_NAME).fields().requiredString("value").endRecord();
		Schema partitionKeyNullable = SchemaBuilder.unionOf().type(partitionKey).and().nullType().endUnion();

		Schema clusteringKey = SchemaBuilder.record(CLUSTERING_KEY_NAME).fields().requiredInt("value").endRecord();
		Schema clusteringKeyNullable = SchemaBuilder.unionOf().type(clusteringKey).and().nullType().endUnion();

		Schema column = SchemaBuilder.record(COLUMN_NAME).fields().optionalString("value").endRecord();
		Schema columnNullable = SchemaBuilder.unionOf().type(column).and().nullType().endUnion();

		Schema result = SchemaBuilder.record("clusterName.keyspace.table.Key")
				.namespace(SCHEMA_NAMESPACE)
				.fields()
				.requiredString("op")
				.name("ts_ms").type(timestampMillisType).noDefault()
				.name(PARTITION_KEY_NAME).type(partitionKeyNullable).noDefault()
				.name(CLUSTERING_KEY_NAME).type(clusteringKeyNullable).noDefault()
				.name(COLUMN_NAME).type(columnNullable).noDefault().endRecord();



		/**
		 * {
		 *         "name": "data",
		 *         "type": "record",
		 *         "fields": [
		 *           {
		 *             "name": "id",
		 *             "type": [
		 *               "null",
		 *               {
		 *             "name": "id",
		 *             "type": "record",
		 *             "fields": [
		 *               {
		 *                 "name":"value",
		 *                 "type": "string"
		 *               }
		 *              ]
		 *             }
		 *           ]
		 *           },
		 */
		System.out.println(result);
	}
}
