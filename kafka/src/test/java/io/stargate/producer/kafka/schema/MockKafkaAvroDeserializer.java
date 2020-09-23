package io.stargate.producer.kafka.schema;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;

public class MockKafkaAvroDeserializer extends KafkaAvroDeserializer {
  private final Schema schema;

  public MockKafkaAvroDeserializer(Schema schema) {
    this.schema = schema;
  }

  @Override
  public Object deserialize(String topic, byte[] bytes) {
    this.schemaRegistry = getMockClient(schema);
    return super.deserialize(topic, bytes);
  }

  private static SchemaRegistryClient getMockClient(final Schema schema$) {
    return new MockSchemaRegistryClient() {
      @Override
      public synchronized Schema getById(int id) {
        return schema$;
      }
    };
  }
}
