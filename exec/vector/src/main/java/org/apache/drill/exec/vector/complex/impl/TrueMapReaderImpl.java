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
package org.apache.drill.exec.vector.complex.impl;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.util.Text;
import org.apache.drill.exec.vector.complex.TrueMapVector;
import org.apache.drill.exec.vector.complex.reader.BaseReader.TrueMapReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.TrueMapWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

import java.math.BigDecimal;

public class TrueMapReaderImpl extends RepeatedMapReaderImpl implements TrueMapReader {

  private static final int NOT_FOUND = -1;

  public TrueMapReaderImpl(TrueMapVector vector) {
    super(vector);
  }

  @Override
  public FieldReader reader(String name){
    assert TrueMapVector.fieldNames.contains(name);
    return super.reader(name);
  }

  @Override
  public int find(String key) {
    Object typifiedKey = getAppropriateKey(key);
    return find(typifiedKey);
  }

  @Override
  public int find(int key) {
    Object typifiedKey = getAppropriateKey(key);
    return find(typifiedKey);
  }

  // todo: actually, key can be of any concrete type, since only literals are passed here?
  // todo: if key-uniqueness is not guaranteed, then return the last one for the key
  private int find(Object key) {
    int idx = vector.getOffsetVector().getAccessor().get(idx() + 1);
    int offset = vector.getOffsetVector().getAccessor().get(idx());
    int index = NOT_FOUND;
    // todo: if key-uniqueness is not guaranteed, then return the last one for the key
    for (int i = 0; i < idx - offset; i++) {
      // todo: probably get key value of byte[] type and compare with actual values?
      Object keyValue = vector.getChild(TrueMapVector.FIELD_KEY_NAME).getAccessor().getObject(offset + i);
      if (keyValue.equals(key)) {
        index = offset + i;
        break;
      }
    }

    return index;
  }

  private Object getAppropriateKey(int key) {
    TypeProtos.MajorType keyType = ((TrueMapVector) vector).getKeyType();
    switch (keyType.getMinorType()) {
      case SMALLINT:
        return (short) key;
      case INT:
        return key;
      case BIGINT:
        return (long) key;
      case FLOAT4:
        return (float) key;
      case FLOAT8:
        return (double) key;
      case VARDECIMAL:
        return BigDecimal.valueOf(key);
      case BIT:
        return key != 0; // todo: !
      default:
        String message = String.format("Unknown value %d for key of type %s", key, keyType.getMinorType().toString());
        throw new IllegalArgumentException(message);
    }
  }

  private Object getAppropriateKey(String key) {
    TypeProtos.MajorType keyType = ((TrueMapVector) vector).getKeyType();
    switch (keyType.getMinorType()) {
      case VARCHAR:
      case VARBINARY:
        return new Text(key);
      case BIT:
        return Boolean.valueOf(key);
      case SMALLINT:
        return Short.valueOf(key);
      case INT:
        return Integer.valueOf(key);
      case BIGINT:
        return Long.valueOf(key);
      case FLOAT4:
        return Float.valueOf(key);
      case FLOAT8:
        return Double.valueOf(key);
      default:
        String message = String.format("Unknown value %s for key of type %s", key, keyType.getMinorType().toString());
        throw new IllegalArgumentException(message);
    }
  }

  @Override
  public void read(String key, ValueHolder holder) {
    read(new Text(key), holder);
  }

  @Override
  public void read(int key, ValueHolder holder) {
    Object key1 = getAppropriateKey(key);
    read(key1, holder);
  }

  private void read(Object key, ValueHolder holder) {
    if (isNull()) {
      return;
    }

    int index = find(key);
    FieldReader valueReader = super.reader(TrueMapVector.FIELD_VALUE_NAME);
    valueReader.setPosition(index);
    // todo: pass the index into the method instead of setting position to the reader
    if (index != NOT_FOUND) {
      valueReader.read(holder);
    }
  }

  // todo: should this be removed?
  // todo: probably make it similarly to primitive types above
  public void read(Object key, ComplexHolder holder) {
    int index = find(key);
    if (index == NOT_FOUND) {
      holder.isSet = 0;
      // todo: include holder.reader = valueReader?
      return;
    }

    holder.isSet = 1;
    FieldReader valueReader = super.reader(TrueMapVector.FIELD_VALUE_NAME);
    valueReader.setPosition(index);
    holder.reader = valueReader;
  }

  @Override
  public void setPosition(int index) {
    if (index == NOT_FOUND) {
      for (FieldReader reader : fields.values()) {
        reader.setPosition(index);
      }
    }
    super.setPosition(index);
  }

  @Override
  public boolean isSet() {
    return currentOffset != NO_VALUES;
  }

  @Override
  public TypeProtos.MajorType getType(){
    return vector.getField().getType();
  }

  @Override
  public void copyAsValue(TrueMapWriter writer) {
    if (isNull()) {
      return;
    }
    ComplexCopier.copy(this, (FieldWriter) writer);
  }
}