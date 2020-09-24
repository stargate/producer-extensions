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

package io.stargate.producer.kafka.helpers;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.stargate.db.Cell;
import org.apache.cassandra.stargate.db.CellValue;
import org.apache.cassandra.stargate.db.RowMutationEvent;
import org.apache.cassandra.stargate.schema.CQLType;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.ColumnMetadata;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.apache.commons.codec.Charsets;
import org.jetbrains.annotations.NotNull;

public class MutationEventHelper {

  @NotNull
  public static RowMutationEvent createRowMutationEvent(
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
  public static CellValue cellValue(Object partitionKeyValue, ColumnMetadata columnMetadata) {
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
  public static ColumnMetadata partitionKey(String partitionKeyName) {

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
  public static ColumnMetadata clusteringKey(String clusteringKeyName) {

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

  public static ColumnMetadata column(String columnName) {
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