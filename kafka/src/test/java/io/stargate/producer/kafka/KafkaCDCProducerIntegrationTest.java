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

import static io.stargate.producer.kafka.schema.Schemas.CLUSTERING_KEY_NAME;
import static io.stargate.producer.kafka.schema.Schemas.COLUMN_NAME;
import static io.stargate.producer.kafka.schema.Schemas.KEY_SCHEMA;
import static io.stargate.producer.kafka.schema.Schemas.PARTITION_KEY_NAME;
import static io.stargate.producer.kafka.schema.Schemas.VALUE_SCHEMA;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Streams;
import io.stargate.producer.kafka.mapping.MappingService;
import io.stargate.producer.kafka.schema.MockKafkaAvroSerializer;
import io.stargate.producer.kafka.schema.MockKeyKafkaAvroDeserializer;
import io.stargate.producer.kafka.schema.MockValueKafkaAvroDeserializer;
import io.stargate.producer.kafka.schema.SchemaProvider;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.ToxiproxyContainer.ContainerProxy;

class KafkaCDCProducerIntegrationTest {

  private static int PORT = 9093;
  private static KafkaContainer kafkaContainer;
  private static ToxiproxyContainer toxiproxyContainer;
  private static ContainerProxy kafkaProxy;

  private static final String TOPIC_NAME = "topic_1";

  @BeforeAll
  public static void setup() {
    Network network = Network.newNetwork();
    kafkaContainer = new KafkaContainer().withNetwork(network);
    toxiproxyContainer = new ToxiproxyContainer().withNetwork(network);
    toxiproxyContainer.start();
    kafkaProxy = toxiproxyContainer.getProxy(kafkaContainer, PORT);
    kafkaContainer.start();
  }

  @AfterAll
  public static void cleanup() {
    kafkaContainer.stop();
    toxiproxyContainer.stop();
  }

  @Test
  public void shouldSendEventWithOnePartitionKeyAndOneValue() throws Exception {
    // given
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 1;
    String columnValue = "col_value";
    MappingService mappingService = mock(MappingService.class);
    SchemaProvider schemaProvider = mock(SchemaProvider.class);
    TableMetadata tableMetadata = mock(TableMetadata.class);

    when(mappingService.getTopicNameFromTableMetadata(tableMetadata)).thenReturn(TOPIC_NAME);

    when(schemaProvider.getKeySchemaForTopic(TOPIC_NAME)).thenReturn(KEY_SCHEMA);
    when(schemaProvider.getValueSchemaForTopic(TOPIC_NAME)).thenReturn(VALUE_SCHEMA);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(mappingService, schemaProvider);
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    kafkaCDCProducer
        .send(
            createRowMutationEvent(
                partitionKeyValue,
                partitionKey(PARTITION_KEY_NAME),
                columnValue,
                column(COLUMN_NAME),
                clusteringKeyValue,
                clusteringKey(CLUSTERING_KEY_NAME),
                tableMetadata))
        .get();

    // then
    GenericRecord expectedKey = new GenericData.Record(KEY_SCHEMA);
    expectedKey.put(PARTITION_KEY_NAME, partitionKeyValue);
    expectedKey.put(CLUSTERING_KEY_NAME, clusteringKeyValue);
    GenericRecord expectedValue = new GenericData.Record(VALUE_SCHEMA);
    expectedValue.put(COLUMN_NAME, columnValue);

    try {
      validateThatWasSendToKafka(expectedKey, expectedValue);
    } finally {
      kafkaCDCProducer.close().get();
    }
  }

  @Test
  public void shouldPropagateErrorWhenKafkaConnectionWasClosed() throws Exception {
    // given
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 1;
    String columnValue = "col_value";
    MappingService mappingService = mock(MappingService.class);
    SchemaProvider schemaProvider = mock(SchemaProvider.class);
    TableMetadata tableMetadata = mock(TableMetadata.class);

    when(mappingService.getTopicNameFromTableMetadata(tableMetadata)).thenReturn(TOPIC_NAME);

    when(schemaProvider.getKeySchemaForTopic(TOPIC_NAME)).thenReturn(KEY_SCHEMA);
    when(schemaProvider.getValueSchemaForTopic(TOPIC_NAME)).thenReturn(VALUE_SCHEMA);

    KafkaCDCProducer kafkaCDCProducer = new KafkaCDCProducer(mappingService, schemaProvider);
    Map<String, Object> properties = createKafkaProducerSettings();
    kafkaCDCProducer.init(properties).get();

    // when
    try {
      // block connections to kafka
      kafkaProxy.setConnectionCut(true);
      assertThatCode(
              () -> {
                kafkaCDCProducer
                    .send(
                        createRowMutationEvent(
                            partitionKeyValue,
                            partitionKey(PARTITION_KEY_NAME),
                            columnValue,
                            column(COLUMN_NAME),
                            clusteringKeyValue,
                            clusteringKey(CLUSTERING_KEY_NAME),
                            tableMetadata))
                    .get();
              })
          .hasRootCauseInstanceOf(TimeoutException.class)
          .hasMessageContaining(String.format("Topic %s not present in metadata", TOPIC_NAME));
    } finally {
      // resume connections
      kafkaProxy.setConnectionCut(false);
      kafkaCDCProducer.close().get();
    }
  }

  @NotNull
  private Map<String, Object> createKafkaProducerSettings() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        String.format("%s:%s", kafkaProxy.getContainerIpAddress(), kafkaProxy.getProxyPort()));
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, MockKafkaAvroSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MockKafkaAvroSerializer.class);
    // lower the max.block to allow faster failure scenario testing
    properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");

    properties.put("schema.registry.url", "mocked");
    return properties;
  }

  @SuppressWarnings("UnstableApiUsage")
  private void validateThatWasSendToKafka(GenericRecord expectedKey, GenericRecord expectedValue) {
    Properties props = new Properties();
    props.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        String.format("%s:%s", kafkaProxy.getContainerIpAddress(), kafkaProxy.getProxyPort()));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MockKeyKafkaAvroDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MockValueKafkaAvroDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put("schema.registry.url", "mocked");

    KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(TOPIC_NAME));

    try {
      await()
          .atMost(Duration.ofSeconds(5))
          .until(
              () -> {
                ConsumerRecords<GenericRecord, GenericRecord> records =
                    consumer.poll(Duration.ofMillis(100));
                return Streams.stream(records)
                    .anyMatch(r -> r.key().equals(expectedKey) && r.value().equals(expectedValue));
              });
    } finally {
      consumer.close();
    }
  }

  @NotNull
  private RowMutationEvent createRowMutationEvent(
      String partitionKeyValue,
      ColumnMetadata partitionKeyMetadata,
      String columnValue,
      ColumnMetadata columnMetadata,
      Integer clusteringKeyValue,
      ColumnMetadata clusteringKeyMetadata,
      TableMetadata tableMetadata) {
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
        return Collections.singletonList(cellValue(partitionKeyValue, partitionKeyMetadata));
      }

      @Override
      public List<CellValue> getClusteringKeys() {
        return Collections.singletonList(cellValue(clusteringKeyValue, clusteringKeyMetadata));
      }

      @Override
      public List<Cell> getCells() {
        return Collections.singletonList(
            new Cell() {
              @Override
              public int getTTL() {
                return 0;
              }

              @Override
              public boolean isNull() {
                return false;
              }

              @Override
              public ColumnMetadata getColumn() {
                return columnMetadata;
              }

              @Override
              public ByteBuffer getValue() {
                return ByteBuffer.wrap(columnValue.getBytes(Charsets.UTF_8));
              }

              @Override
              public Object getValueObject() {
                return columnValue;
              }
            });
      }
    };
  }

  @NotNull
  private CellValue cellValue(Object partitionKeyValue, ColumnMetadata columnMetadata) {
    return new CellValue() {
      @Override
      public ByteBuffer getValue() {
        return null;
      }

      @Override
      public Object getValueObject() {
        return partitionKeyValue;
      }

      @Override
      public ColumnMetadata getColumn() {
        return columnMetadata;
      }
    };
  }

  @NotNull
  private ColumnMetadata partitionKey(String partitionKeyName) {

    return new ColumnMetadata() {
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
    };
  }

  @NotNull
  private ColumnMetadata clusteringKey(String clusteringKeyName) {

    return new ColumnMetadata() {
      @Override
      public Kind getKind() {
        return Kind.PARTITION_KEY;
      }

      @Override
      public String getName() {
        return clusteringKeyName;
      }

      @Override
      public CQLType getType() {
        return Native.TEXT;
      }
    };
  }

  private ColumnMetadata column(String columnName) {
    return new ColumnMetadata() {
      @Override
      public Kind getKind() {
        return Kind.REGULAR;
      }

      @Override
      public String getName() {
        return columnName;
      }

      @Override
      public CQLType getType() {
        return Native.TEXT;
      }
    };
  }
}
