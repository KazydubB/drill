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
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.vector.complex.TrueMapVector;
import org.apache.drill.exec.vector.complex.impl.TrueMapWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.Collection;
import java.util.Collections;

public class DrillMapGroupConverter extends DrillParquetGroupConverter {

  public DrillMapGroupConverter(String name, OutputMutator mutator, BaseWriter.MapWriter mapWriter, GroupType schema,
                    Collection<SchemaPath> columns, OptionManager options,
                    ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates) {
    this.mutator = mutator;
    this.containsCorruptedDates = containsCorruptedDates;
    this.options = options;
    GroupType type = schema.getType(0).asGroupType();
    // todo: getType by name or by ordinal is OK?
    this.baseWriter = mapWriter.trueMap(name, getMajorType(type.getType(0), options), getMajorType(type.getType(1), options));

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
        if (valueType.getOriginalType() == OriginalType.MAP) { // todo: add additoinal condition to check if valuType.minorType is TRUEMAP
          // todo: wrap valueWriter?
          valueConverter = new DrillMapGroupConverter(valueType.getName(), mutator, mapWriter, groupType, columns, options, containsCorruptedDates);
        // } else if (valueType.getOriginalType() == OriginalType.LIST) { // todo: uncomment when the time has come
          // todo: implement when DRILL-7268 is merged
        } else {
          // BaseWriter.MapWriter newMapWriter = groupType.getRepetition() == Type.Repetition.REPEATED ?
              // mapWriter.list(valueType.getName()).map() : mapWriter.map(valueType.getName()); // todo: uncomment this if anything
//          BaseWriter.MapWriter newMapWriter = mapWriter.getValueWriter(); // todo: for types other then MAP it may be different
//          valueConverter = new DrillParquetGroupConverter(mutator, newMapWriter, groupType, columns, options, containsCorruptedDates); // todo: resolve it for LIST (and see if
          // it works for TRUEMAP)
          valueConverter = new DrillParquetGroupConverter(mutator, mapWriter.map(TrueMapVector.FIELD_VALUE_NAME), groupType,
              columns, options, containsCorruptedDates, false, "gawgawgawg");
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
// todo: can this be discarded?
  private static TypeProtos.MajorType getMajorType(Type t, OptionManager options) {
    if (!t.isPrimitive()) {
      return getComplexMajorType(t.asGroupType(), options);
    }

    TypeProtos.DataMode mode = getMode(t);

    PrimitiveType type = (PrimitiveType) t;
    TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder();
    switch(type.getPrimitiveTypeName()) {
      case INT32: {
        if (type.getOriginalType() == null) {
          builder.setMinorType(TypeProtos.MinorType.INT);
          break;
        }
        switch(type.getOriginalType()) {
          case UINT_8 :
          case UINT_16:
          case UINT_32:
          case INT_8  :
          case INT_16 :
          case INT_32 :
            builder.setMinorType(TypeProtos.MinorType.INT);
            break;
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            builder.setMinorType(TypeProtos.MinorType.VARDECIMAL)
                .setScale(type.getDecimalMetadata().getScale())
                .setScale(type.getDecimalMetadata().getPrecision());
            break;
          }
        }
      }
      case INT64: {
        if (type.getOriginalType() == null) {
          builder.setMinorType(TypeProtos.MinorType.BIGINT);
          break;
        }
        switch(type.getOriginalType()) {
          case UINT_64:
          case INT_64 :
            builder.setMinorType(TypeProtos.MinorType.BIGINT);
            break;
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            builder.setMinorType(TypeProtos.MinorType.VARDECIMAL)
                .setScale(type.getDecimalMetadata().getScale())
                .setScale(type.getDecimalMetadata().getPrecision());
            break;
          }
          default: {
            throw new UnsupportedOperationException("Unsupported type " + type.getOriginalType());
          }
        }
      }
      case FLOAT:
        builder.setMinorType(TypeProtos.MinorType.FLOAT4);
        break;
      case DOUBLE:
        builder.setMinorType(TypeProtos.MinorType.FLOAT8);
        break;
      case BOOLEAN:
        builder.setMinorType(TypeProtos.MinorType.BIT);
        break;
      case BINARY: {
        if (type.getOriginalType() == null) {
          builder.setMinorType(TypeProtos.MinorType.VARBINARY);
          break;
        }
        switch(type.getOriginalType()) {
          case UTF8:
          case ENUM:
            builder.setMinorType(TypeProtos.MinorType.VARCHAR);
            break;
          // See DRILL-4184 and DRILL-4834. Support for this is added using new VarDecimal type.
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            builder.setMinorType(TypeProtos.MinorType.VARDECIMAL)
                .setScale(type.getDecimalMetadata().getScale())
                .setScale(type.getDecimalMetadata().getPrecision());
            break;
          }
          default: {
            // throw new UnsupportedOperationException("Unsupported type " + type.getOriginalType());
            builder.setMinorType(TypeProtos.MinorType.VARBINARY);
            break;
          }
        }
        break;
      }
      case FIXED_LEN_BYTE_ARRAY:
        switch (type.getOriginalType()) {
          case DECIMAL: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            builder.setMinorType(TypeProtos.MinorType.VARDECIMAL)
                .setScale(type.getDecimalMetadata().getScale())
                .setScale(type.getDecimalMetadata().getPrecision());
            break;
          }
          default: {
            ParquetReaderUtility.checkDecimalTypeEnabled(options);
            builder.setMinorType(TypeProtos.MinorType.VARBINARY);
            break;
          }
        }
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type.getPrimitiveTypeName());
    }

    return builder.setMode(mode)
        .build();
  }

  private static TypeProtos.MajorType getComplexMajorType(GroupType groupType, OptionManager options) {
    TypeProtos.MinorType minorType;
    TypeProtos.DataMode mode = getMode(groupType);

    if (groupType.getOriginalType() == null) {
      minorType = TypeProtos.MinorType.MAP;
    } else {
      switch (groupType.getOriginalType()) {
        case MAP:
          minorType = TypeProtos.MinorType.TRUEMAP;
          break;
        case LIST:
          // todo: handle list (repeated) types here...
//          minorType = TypeProtos.MinorType.LIST;
          // extract 'bag' group containing array elements
          GroupType nestedType = groupType.getType(0).asGroupType();
          Type elementType = nestedType.getType(0);
          minorType = getMajorType(elementType, options).getMinorType();
          mode = TypeProtos.DataMode.REPEATED; // todo: is this correct?
          break;
        default:
          minorType = TypeProtos.MinorType.MAP;
          break;
      }
    }

    return TypeProtos.MajorType.newBuilder()
        .setMinorType(minorType)
        .setMode(mode)
        .build();
  }

  private static TypeProtos.DataMode getMode(Type type) { // todo: move to HiveUtilities
    TypeProtos.DataMode mode;
    switch (type.getRepetition()) { // todo: this should be present somewhere
      case REPEATED:
        mode = TypeProtos.DataMode.REPEATED;
        break;
      case OPTIONAL:
        mode = TypeProtos.DataMode.OPTIONAL;
        break;
      case REQUIRED:
        mode = TypeProtos.DataMode.REQUIRED;
        break;
      default:
        throw new IllegalArgumentException("Unknown mode " + type.getRepetition());
    }
    return mode;
  }
}
