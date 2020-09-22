/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.producer.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.stargate.producer.kafka.mapping.MappingService;
import io.stargate.producer.kafka.schema.SchemaProvider;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.stargate.db.Cell;
import org.apache.cassandra.stargate.db.CellValue;
import org.apache.cassandra.stargate.db.RowMutationEvent;
import org.apache.cassandra.stargate.schema.CQLType;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.ColumnMetadata;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.apache.commons.codec.Charsets;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;

class KafkaCDCProducerIntegrationTest {

  private static final int KAFKA_PORT = 9093;

  private static KafkaContainer kafkaContainer;

  private static final String TOPIC_NAME = "topic_1";

  @BeforeAll
  public static void setup() {
    Network network = Network.newNetwork();
    //		.withExposedPorts(KAFKA_PORT)
    kafkaContainer = new KafkaContainer().withNetwork(network);
    kafkaContainer.start();
  }

  @AfterAll
  public static void cleanup() {
    kafkaContainer.stop();
  }

  @Test
  public void shouldSendEventWithOnePartitionKeyAndOneValue() {
    // given
    String partitionKeyName = "pk_1";
    String partitionKeyValue = "pk_value";
    String columnName = "col_1";
    String columnValue = "col_value";
    MappingService mappingService = mock(MappingService.class);
    SchemaProvider schemaProvider = mock(SchemaProvider.class);
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.getPartitionKeys()).thenReturn(stringPartitionKey(partitionKeyName));
    when(mappingService.getPartitionKeyName(tableMetadata)).thenReturn(TOPIC_NAME);
    when(mappingService.getPartitionKeyName(tableMetadata)).thenReturn(partitionKeyName);

    Schema keySchema =
        SchemaBuilder.record("key").fields().requiredString(partitionKeyName).endRecord();

    Schema valueSchema =
        SchemaBuilder.record("value").fields().requiredString(columnName).endRecord();

    // todo verify value
    when(schemaProvider.getKeySchemaForTableMetadata(TOPIC_NAME)).thenReturn(keySchema);
    when(schemaProvider.getValueSchemaForTableMetadata(TOPIC_NAME)).thenReturn(valueSchema);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(mappingService, schemaProvider);
    Map<String, Object> properties = new HashMap<>();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    kafkaCDCProducer.init(properties);

    // when
    kafkaCDCProducer.send(createRowMutationEvent(partitionKeyValue, tableMetadata));

    // then
    GenericRecord expectedKey = new GenericData.Record(keySchema);
    expectedKey.put(partitionKeyName, partitionKeyValue);
    GenericRecord expectedValue = new GenericData.Record(valueSchema);
    expectedValue.put(columnName, columnValue);
    validateThatWasSendToKafka(expectedKey, expectedValue);
  }

  private void validateThatWasSendToKafka(GenericRecord expectedKey, GenericRecord expectedValue) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

    // todo add validation
    KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(TOPIC_NAME));
    while (true) {
      ConsumerRecords<GenericRecord, GenericRecord> recs = consumer.poll(Duration.ofSeconds(10));
      for (ConsumerRecord<GenericRecord, GenericRecord> rec : recs) {
        System.out.printf(
            "{AvroUtilsConsumerUser}: Recieved [key= %s, value= %s]\n", rec.key(), rec.value());
      }
    }
  }

  @NotNull
  private RowMutationEvent createRowMutationEvent(
      String partitionKeyValue, TableMetadata tableMetadata) {
    return new RowMutationEvent() {
      @Override
      public TableMetadata getTable() {
        return tableMetadata;
      }

      @Override
      public long getTimestamp() {
        return 0;
      }

      @Override
      public List<CellValue> getPartitionKeys() {
        return Collections.singletonList(cellValue(partitionKeyValue));
      }

      @Override
      public List<CellValue> getClusteringKeys() {
        return null;
      }

      @Override
      public List<Cell> getCells() {
        return null;
      }
    };
  }

  @NotNull
  private CellValue cellValue(String partitionKeyValue) {
    return new CellValue() {
      @Override
      public ByteBuffer getValue() {
        return ByteBuffer.wrap(partitionKeyValue.getBytes(Charsets.UTF_8));
      }

      @Override
      public Object getValueObject() {
        return null;
      }
    };
  }

  @NotNull
  private List<ColumnMetadata> stringPartitionKey(String partitionKeyName) {
    return Collections.singletonList(
        new ColumnMetadata() {
          @Override
          public Kind getKind() {
            return Kind.PARTITION_KEY;
          }

          @Override
          public String getName() {
            return partitionKeyName;
          }

          @Override
          public CQLType getType() {
            return Native.TEXT;
          }
        });
  }
}
