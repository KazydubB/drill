/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet2;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.vector.complex.TrueMapVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.TrueMapWriter;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import java.util.Collection;
import java.util.Collections;

class DrillMapGroupConverter extends DrillParquetGroupConverter {

  private final TrueMapWriter writer;

  DrillMapGroupConverter(OutputMutator mutator, TrueMapWriter mapWriter, GroupType schema,
                    Collection<SchemaPath> columns, OptionManager options,
                    ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates) {
    super(mutator, mapWriter, options, containsCorruptedDates);
    writer = mapWriter;

    GroupType type = schema.getType(0).asGroupType();
    Converter innerConverter = new KeyValueGroupConverter(mutator, type, columns, options, containsCorruptedDates);
    converters = Collections.singletonList(innerConverter);
  }

  @Override
  public void start() {
    writer.start();
  }

  @Override
  public void end() {
    writer.end();
  }

  private class KeyValueGroupConverter extends DrillParquetGroupConverter {

    KeyValueGroupConverter(OutputMutator mutator, GroupType schema, Collection<SchemaPath> columns,
                           OptionManager options, ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates) {
      super(mutator, writer, options, containsCorruptedDates);

      Type keyType = schema.getType(0);
      if (!keyType.isPrimitive()) {
        throw new DrillRuntimeException("Map supports primitive key only. Found: " + keyType);
      } else {
        Converter keyConverter = getConverterForType(TrueMapVector.FIELD_KEY_NAME, keyType.asPrimitiveType());
        converters.add(keyConverter);
      }
      Type valueType = schema.getType(1);

      Converter valueConverter;
      if (!valueType.isPrimitive()) {
        GroupType groupType = valueType.asGroupType();
        if (isLogicalMapType(groupType)) {
          TrueMapWriter valueWriter = writer.trueMap(TrueMapVector.FIELD_VALUE_NAME);
          valueConverter =
            new DrillMapGroupConverter(mutator, valueWriter, groupType, columns, options, containsCorruptedDates);
        } else {
          BaseWriter valueWriter = isLogicalListType(groupType)
                  ? writer.list(TrueMapVector.FIELD_VALUE_NAME)
                  : writer.map(TrueMapVector.FIELD_VALUE_NAME);
          valueConverter = new DrillParquetGroupConverter(mutator, valueWriter, groupType, columns, options,
                  containsCorruptedDates, true, "KeyValueGroupConverter");
        }
      } else {
        valueConverter = getConverterForType(TrueMapVector.FIELD_VALUE_NAME, valueType.asPrimitiveType());
      }
      converters.add(valueConverter);
    }

    @Override
    public void start() {
      writer.startKeyValuePair();
    }

    @Override
    public void end() {
      writer.endKeyValuePair();
    }
  }
}
