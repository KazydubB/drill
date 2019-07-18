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

  DrillMapGroupConverter(String name, OutputMutator mutator, BaseWriter.MapWriter mapWriter, GroupType schema,
                    Collection<SchemaPath> columns, OptionManager options,
                    ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates) {
    this.mutator = mutator;
    this.containsCorruptedDates = containsCorruptedDates;
    this.options = options;
    GroupType type = schema.getType(0).asGroupType();
    this.baseWriter = mapWriter.trueMap(name);

    Converter innerConverter = new KeyValueGroupConverter(mutator, (TrueMapWriter) this.baseWriter, type, columns, options, containsCorruptedDates);
    converters = Collections.singletonList(innerConverter);
  }

  private static class KeyValueGroupConverter extends DrillParquetGroupConverter {

    KeyValueGroupConverter(OutputMutator mutator, TrueMapWriter mapWriter, GroupType schema,
                           Collection<SchemaPath> columns, OptionManager options,
                           ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates) {
      super(mutator, mapWriter, options, containsCorruptedDates);
      Type keyType = schema.getType(0);
      if (!keyType.isPrimitive()) { // todo; change with Precondition
        throw new DrillRuntimeException("Map supports primitive key only. Found: " + keyType);
      } else {
        Converter keyConverter = getConverterForType(TrueMapVector.FIELD_KEY_NAME, keyType.asPrimitiveType());
        converters.add(keyConverter);
      }
      Type valueType = schema.getType(1);
      Converter valueConverter;
      if (!valueType.isPrimitive()) { // todo: change complex case
        GroupType groupType = valueType.asGroupType();
        if (isLogicalMapType(groupType)) { // todo: add additoinal condition to check if valuType.minorType is TRUEMAP
          // todo: wrap valueWriter?
          valueConverter = new DrillMapGroupConverter(valueType.getName(), mutator, mapWriter, groupType, columns, options, containsCorruptedDates);
        } else if (isLogicalListType(groupType)) {
          valueConverter = new DrillParquetGroupConverter(mutator, mapWriter.list(TrueMapVector.FIELD_VALUE_NAME), groupType,
              columns, options, containsCorruptedDates, true, "wagawgawgawg");
        } else {
          valueConverter = new DrillParquetGroupConverter(mutator, mapWriter.map(TrueMapVector.FIELD_VALUE_NAME), groupType,
              columns, options, containsCorruptedDates, true, "gawgawgawg");
        }
      } else {
        valueConverter = getConverterForType(TrueMapVector.FIELD_VALUE_NAME, valueType.asPrimitiveType());
      }
      converters.add(valueConverter);
    }

    @Override
    public void start() {
      ((TrueMapWriter) baseWriter).startKeyValuePair();
    }

    @Override
    public void end() {
      ((TrueMapWriter) baseWriter).endKeyValuePair();
    }
  }
}
