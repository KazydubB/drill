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
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.TrueMapVector;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

public class TrueMapWriter extends RepeatedMapWriter {

  private final TrueMapVector container;

  // todo: change to int lastSet
  private int currentRow = -1;
  // todo: use currentChildIndex from RepeatedMapWriter
  private int length = -1; // (designates length for current row)
  private boolean rowStarted;

  public TrueMapWriter(TrueMapVector container, FieldWriter parent, boolean unionEnabled) {
    super(container, parent, unionEnabled);
    this.container = container;
  }

  public TrueMapWriter(TrueMapVector container, FieldWriter parent) {
    this(container, parent, false);
  }

  public void startKeyValuePair() {
    // todo: should entry be marked as not-set (null) explicitly?
    int idx = idx();
//    super.setPosition(currentRow);
    int index = getPosition();
    checkStarted();
    for (FieldWriter writer : fields.values()) {
      writer.setPosition(index); // todo: variable
    }
    // setPosition(getPosition()); // todo: remove?
  }

  // todo: discard
  @Deprecated
  private int getPosition() { // todo: rename to index?
    checkStarted();
    // todo: change currentRow with idx()?
    int offset = container.getInnerOffset(currentRow); // todo: there can be problems connected to this
    return offset + length;
  }

  public void endKeyValuePair() {
    checkStarted();

    length++;
  }

  @Override
  public TrueMapWriter trueMap(String name, TypeProtos.MajorType keyType, TypeProtos.MajorType valueType) {
    // todo: consider the same assertion for list(String), list()??, map(String) methods
    assert TrueMapVector.FIELD_VALUE_NAME.equals(name) : "Only value field is allowed in TrueMap";
// todo: change to FieldWriter?
    return super.trueMap(name, keyType, valueType);
  }

  @Override
  public FieldWriter getKeyWriter() {
    return fields.get(TrueMapVector.FIELD_KEY_NAME);
  }

  @Override
  public FieldWriter getValueWriter() {
    return fields.get(TrueMapVector.FIELD_VALUE_NAME);
  }

  public void setValueCount(int count) {
    container.getMutator().setValueCount(count);
  }

  @Override
  public void start() {
    currentRow++; // todo: currentRow = idx();
    length = 0;
    rowStarted = true;
  }

  @Override
  public void end() {
    checkStarted();

    int offset = container.getInnerOffset(currentRow); // todo: this may be not true in a case when currentRow is
    rowStarted = false;
    int currentOffset = length;
    currentOffset += offset;
//    } // todo: decide if this should be done for currentRow + 1 or not
    container.setInnerOffset(currentRow + 1, currentOffset);
  }

  public void put(int outputIndex, Object key, Object value) { // todo: outputIndex?
    checkStarted();

    int index = getPosition();
    // todo: change this?
    setValue(container.getKeys(), key, index);
    setValue(container.getValues(), value, index);
    length++;
  }

  private void setValue(ValueVector vector, Object value, int index) {
    if (vector instanceof NullableIntVector) { // todo: not instanceof but type?
      ((NullableIntVector) vector).getMutator().setSafe(index, (int) value);
    } else if (vector instanceof NullableVarCharVector) {
      byte[] bytes = (byte[]) value;
      ((NullableVarCharVector) vector).getMutator().setSafe(index, bytes, 0, bytes.length);
    }
  }

  public void put(int outputIndex, ValueHolder keyHolder, ValueHolder valueHolder) { // todo: outputIndex?
    checkStarted();

    int index = getPosition();
    getKeyWriter().setPosition(index);
    getValueWriter().setPosition(index);
  }

  public TypeProtos.MajorType getKeyType() {
    return container.getKeyType();
  }

  public TypeProtos.MajorType getValueType() {
    return container.getValueType();
  }

  private void checkStarted() {
    assert rowStarted : "Must start row (startRow()) before";
  }
}
