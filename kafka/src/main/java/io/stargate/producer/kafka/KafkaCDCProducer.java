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
import io.stargate.producer.kafka.converters.future.CompletablePromise;
import io.stargate.producer.kafka.mapping.MappingService;
import io.stargate.producer.kafka.schema.SchemaProvider;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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

  private Consumer<Object> toVoid() {
    return (producer) -> {};
  }

  @Override
  protected CompletableFuture<Void> createTableSchemaAsync(TableMetadata tableMetadata) {
    // todo
    return CompletableFuture.completedFuture(null);
  }

  @Override
  protected CompletableFuture<Void> send(MutationEvent mutationEvent) {
    return kafkaProducer.thenCompose(
        producer -> {
          if (mutationEvent instanceof RowMutationEvent) {
            return handleRowMutationEvent((RowMutationEvent) mutationEvent, producer);
          } else {
            return handleNotSupportedEventType(mutationEvent);
          }
        });
  }

  @NotNull
  private CompletionStage<Void> handleRowMutationEvent(
      RowMutationEvent mutationEvent, KafkaProducer<GenericRecord, GenericRecord> producer) {
    ProducerRecord<GenericRecord, GenericRecord> producerRecord = toProducerRecord(mutationEvent);
    return CompletablePromise.fromFuture(producer.send(producerRecord, new KafkaProducerCallback()))
        .thenAccept(toVoid());
  }

  @NotNull
  private CompletionStage<Void> handleNotSupportedEventType(MutationEvent mutationEvent) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    result.completeExceptionally(
        new UnsupportedOperationException(
            "The MutationEvent of type: " + mutationEvent.getClass() + " is not supported."));
    return result;
  }

  private ProducerRecord<GenericRecord, GenericRecord> toProducerRecord(
      RowMutationEvent mutationEvent) {
    String topicName = mappingService.getTopicNameFromTableMetadata(mutationEvent.getTable());

    GenericRecord key = constructKey(mutationEvent, topicName);
    GenericRecord value = constructValue(mutationEvent, topicName);
    return new ProducerRecord<>(topicName, key, value);
  }

  @SuppressWarnings("UnstableApiUsage")
  private GenericRecord constructGenericRecord(
      List<ColumnMetadata> columnsMetadata, List<? extends CellValue> cellValues, Schema schema) {

    GenericRecord value = new GenericData.Record(schema);
    List<ColumnMetadataWithCellValue> columns =
        Streams.zip(columnsMetadata.stream(), cellValues.stream(), ColumnMetadataWithCellValue::new)
            .collect(Collectors.toList());
    for (ColumnMetadataWithCellValue column : columns) {
      value.put(column.columnMetadata.getName(), column.cellValue.getValueObject());
    }
    return value;
  }

  @NotNull
  private GenericRecord constructValue(RowMutationEvent mutationEvent, String topicName) {
    return constructGenericRecord(
        mutationEvent.getTable().getColumns(),
        mutationEvent.getCells(),
        schemaProvider.getValueSchemaForTopic(topicName));
  }

  @NotNull
  private GenericRecord constructKey(RowMutationEvent mutationEvent, String topicName) {
    return constructGenericRecord(
        mutationEvent.getTable().getPartitionKeys(),
        mutationEvent.getPartitionKeys(),
        schemaProvider.getKeySchemaForTopic(topicName));
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
