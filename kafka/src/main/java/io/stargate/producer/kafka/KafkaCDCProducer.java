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

import com.google.common.collect.Streams;
import io.stargate.db.cdc.SchemaAwareCDCProducer;
import io.stargate.producer.kafka.mapping.MappingService;
import io.stargate.producer.kafka.schema.SchemaProvider;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.stargate.db.CellValue;
import org.apache.cassandra.stargate.db.MutationEvent;
import org.apache.cassandra.stargate.db.RowMutationEvent;
import org.apache.cassandra.stargate.schema.ColumnMetadata;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;

public class KafkaCDCProducer extends SchemaAwareCDCProducer {

  private final MappingService mappingService;

  private final SchemaProvider schemaProvider;

  private CompletableFuture<KafkaProducer<GenericRecord, GenericRecord>> kafkaProducer;

  public KafkaCDCProducer(MappingService mappingService, SchemaProvider schemaProvider) {
    this.mappingService = mappingService;
    this.schemaProvider = schemaProvider;
  }

  @Override
  public CompletableFuture<Void> init(Map<String, Object> options) {
    kafkaProducer = CompletableFuture.supplyAsync(() -> new KafkaProducer<>(options));
    return kafkaProducer.thenAccept(toVoid());
  }

  private Consumer<KafkaProducer<GenericRecord, GenericRecord>> toVoid() {
    return (producer) -> {};
  }

  @Override
  protected CompletableFuture<Void> createTableSchemaAsync(TableMetadata tableMetadata) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  protected CompletableFuture<Void> send(MutationEvent mutationEvent) {
    return kafkaProducer.thenAccept(
        producer -> {
          if (mutationEvent instanceof RowMutationEvent) {
            ProducerRecord<GenericRecord, GenericRecord> producerRecord =
                toProducerRecord((RowMutationEvent) mutationEvent);
            try {
              // todo to CompletableFuture and flatten
              RecordMetadata recordMetadata =
                  producer.send(producerRecord, new KafkaProducerCallback()).get();
              System.out.println(recordMetadata);
            } catch (InterruptedException | ExecutionException e) {
              e.printStackTrace();
            }
          }
        });
  }

  private ProducerRecord<GenericRecord, GenericRecord> toProducerRecord(
      RowMutationEvent mutationEvent) {
    String topicName = mappingService.getTopicNameFromTableMetadata(mutationEvent.getTable());

    GenericRecord key = constructKey(mutationEvent, topicName);
    GenericRecord value = constructValue(mutationEvent, topicName);
    return new ProducerRecord<>(topicName, key, value);
  }

  private GenericRecord constructValue(RowMutationEvent mutationEvent, String topicName) {
    Schema valueSchema = schemaProvider.getValueSchemaForTopic(topicName);
    GenericRecord value = new GenericData.Record(valueSchema);
    List<ColumnMetadataWithCellValue> columns =
        Streams.zip(
                mutationEvent.getTable().getColumns().stream(),
                mutationEvent.getCells().stream(),
                ColumnMetadataWithCellValue::new)
            .collect(Collectors.toList());
    for (ColumnMetadataWithCellValue column : columns) {
      value.put(column.columnMetadata.getName(), column.cellValue.getValueObject());
    }
    return value;
  }

  @NotNull
  @SuppressWarnings("UnstableApiUsage")
  private GenericRecord constructKey(RowMutationEvent mutationEvent, String topicName) {

    List<ColumnMetadataWithCellValue> partitionKeys =
        Streams.zip(
                mutationEvent.getTable().getPartitionKeys().stream(),
                mutationEvent.getPartitionKeys().stream(),
                ColumnMetadataWithCellValue::new)
            .collect(Collectors.toList());

    Schema keySchema = schemaProvider.getKeySchemaForTopic(topicName);
    GenericRecord key = new GenericData.Record(keySchema);

    for (ColumnMetadataWithCellValue pk : partitionKeys) {
      key.put(pk.columnMetadata.getName(), pk.cellValue.getValueObject());
    }
    return key;
  }

  @Override
  public void close() throws Exception {
    kafkaProducer
        .thenAccept(
            producer -> {
              producer.flush();
              producer.close();
            })
        .get();
  }

  private static class ColumnMetadataWithCellValue {

    private final ColumnMetadata columnMetadata;

    private final CellValue cellValue;

    public ColumnMetadataWithCellValue(ColumnMetadata columnMetadata, CellValue cellValue) {

      this.columnMetadata = columnMetadata;
      this.cellValue = cellValue;
    }
  }
}
