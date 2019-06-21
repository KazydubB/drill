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
// todo: uncomment
import org.apache.drill.exec.vector.complex.impl.TrueMapWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.Collection;
import java.util.Collections;

public class DrillMapGroupConverter extends DrillParquetGroupConverter {

  // todo: uncomment
  public DrillMapGroupConverter(String name, OutputMutator mutator, BaseWriter.MapWriter mapWriter, GroupType schema, // GroupType keyValueType,
                    // DrillParquetGroupConverter parent,
                    Collection<SchemaPath> columns, OptionManager options,
                    ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates) {
    this.mutator = mutator;
    this.containsCorruptedDates = containsCorruptedDates;
    this.options = options;
    GroupType type = schema.getType(0).asGroupType();
    // Preconditions.checkArgument(type.getOriginalType() == OriginalType.MAP_KEY_VALUE, "MAP_KEY_VALUE is expected");
//     this.mapWriter = mapWriter.trueMap(name, getMajorType(type.getType(0), options), getMajorType(type.getType(1), options)); // todo: was before
     this.baseWriter = mapWriter.trueMap(name, getMajorType(type.getType(0), options), getMajorType(type.getType(1), options)); // todo: was before
//    // --------------------------- todo: experimental
//    TypeProtos.MajorType keyType = getMajorType(type.getType(0), options);
//    TypeProtos.MajorType valueType = getMajorType(type.getType(1), options);
//    this.baseWriter = mapWriter.trueMap(name, keyType, valueType);
//    // --------------------------- todo: experimental end
//    Converter innerConverter = new KeyValueGroupConverter(mutator, (TrueMapWriter) this.baseWriter, type, columns, options, containsCorruptedDates);
//    List<TypeProtos.MajorType> keyType = getMajorType(type.getType(0), options);
//    List<TypeProtos.MajorType> valueType = getMajorType(type.getType(1), options);
//    this.mapWriter = mapWriter.trueMap(name, keyType.get(0), valueType.get(0));
    // --------------------------- todo: experimental end
    // todo: maybe pass list of valueTypes to KeyValueGroupConverter?
//    if (mapWriter.getField().getType().getMinorType() == TypeProtos.MinorType.TRUEMAP
//        && this.mapWriter.getField().getType().getMinorType() == TypeProtos.MinorType.TRUEMAP) {
//      ((TrueMapWriter) mapWriter).allocateValueWriter();
//    }
    Converter innerConverter = new KeyValueGroupConverter(mutator, (TrueMapWriter) this.baseWriter, type, columns, options, containsCorruptedDates);
    converters = Collections.singletonList(innerConverter); // todo: uncomment
  }

  private static class KeyValueGroupConverter extends DrillParquetGroupConverter {

    KeyValueGroupConverter(OutputMutator mutator, TrueMapWriter mapWriter, GroupType schema, // GroupType keyValueType,
                           Collection<SchemaPath> columns, OptionManager options,
                           ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates) {
      super(mutator, mapWriter, schema, columns, options, containsCorruptedDates, false);
      Type keyType = schema.getType(0);
      if (!keyType.isPrimitive()) { // todo; change with Precondition
        throw new DrillRuntimeException("Map supports primitive key only. Found: " + keyType);
      } else {
        Converter keyConverter = getConverterForType(mapWriter.getKeyWriter());
        converters.add(keyConverter);
      }
      Type valueType = schema.getType(1);
      Converter valueConverter;
      if (!valueType.isPrimitive()) { // todo: change complex case
        GroupType groupType = valueType.asGroupType();
        if (valueType.getOriginalType() == OriginalType.MAP) { // todo: add additoinal condition to check if valuType.minorType is TRUEMAP
          // todo: wrap valueWriter?
          valueConverter = new DrillMapGroupConverter(valueType.getName(), mutator, mapWriter, groupType, columns, options, containsCorruptedDates);
          //mapWriter.allocateValueWriter(); // todo: revise this!!!
        // } else if (valueType.getOriginalType() == OriginalType.LIST) { // todo: uncomment when the time has come
          // todo: implement when DRILL-7268 is merged
        } else {
          // BaseWriter.MapWriter newMapWriter = groupType.getRepetition() == Type.Repetition.REPEATED ?
              // mapWriter.list(valueType.getName()).map() : mapWriter.map(valueType.getName()); // todo: uncomment this if anything
          BaseWriter.MapWriter newMapWriter = mapWriter.getValueWriter(); // todo: for types other then MAP it may be different
          valueConverter = new DrillParquetGroupConverter(mutator, newMapWriter, groupType, columns, options, containsCorruptedDates, false, "what the fuck"); // todo: resolve it
          // for LIST
          // (and
          // see if
          // it works for TRUEMAP)
        }
      } else {
        valueConverter = getConverterForType(mapWriter.getValueWriter());
      }
      converters.add(valueConverter);
    }

    @SuppressWarnings("resource")
    protected PrimitiveConverter getConverterForType(FieldWriter writer) {
      TypeProtos.MajorType type = writer.getField().getType();
      switch(type.getMinorType()) {
        case INT:
          return new DrillIntConverter(writer);
        case BIGINT:
          return new DrillBigIntConverter(writer);
        case FLOAT4:
          return new DrillFloat4Converter(writer);
        case FLOAT8:
          return new DrillFloat8Converter(writer);
        case BIT:
          return new DrillBoolConverter(writer);
        case VARBINARY:
          return new DrillVarBinaryConverter(writer, mutator.getManagedBuffer());
        case VARCHAR:
          return new DrillVarCharConverter(writer, mutator.getManagedBuffer());
        case VARDECIMAL:
          return new DrillVarDecimalConverter(writer, type.getPrecision(), type.getScale(), mutator.getManagedBuffer());
        // todo: add other types (FIXEDBINARY etc.)
        default:
          throw new UnsupportedOperationException("Unsupported type: " + type);
      }
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
//    TypeProtos.DataMode mode;
    switch (groupType.getOriginalType()) {
//      case LIST:
//        //minorType = TypeProtos.MinorType.LIST;
//        Type elementType = groupType.getType(0).asGroupType().getType(0);
//        List<TypeProtos.MajorType> elementType1 = getMajorType(elementType, options);
//        minorType = elementType1.getMinorType();
//        mode = TypeProtos.DataMode.REPEATED;
//        break;
      case MAP:
        // List<TypeProtos.MajorType> mapType = new ArrayList<>(3);
        //minorType = TypeProtos.MinorType.TRUEMAP;
        // mode = getMode(groupType);
//        TypeProtos.MajorType mt = TypeProtos.MajorType.newBuilder()
        minorType = TypeProtos.MinorType.TRUEMAP;
        break;
      case LIST:
        minorType = TypeProtos.MinorType.LIST;
        break;
//        mapType.add(mt);
//        Type mapKeyType = groupType.getType(0).asGroupType().getType(0);
//        Type mapValueType = groupType.getType(0).asGroupType().getType(1);
//        mapType.addAll(getMajorType(mapKeyType, options));
//        mapType.addAll(getMajorType(mapValueType, options));
//        return mapType;
      default:
        minorType = TypeProtos.MinorType.MAP;
        // mode = getMode(groupType);
        break;
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
