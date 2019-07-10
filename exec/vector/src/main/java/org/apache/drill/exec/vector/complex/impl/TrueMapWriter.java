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
import org.apache.drill.exec.expr.holders.TrueMapHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.vector.complex.TrueMapVector;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

public class TrueMapWriter extends RepeatedMapWriter {

  final TrueMapVector container;
  private boolean rowStarted;

  public TrueMapWriter(TrueMapVector container, FieldWriter parent, boolean unionEnabled) {
    super(container, parent, unionEnabled);
    this.container = container;
  }

  public TrueMapWriter(TrueMapVector container, FieldWriter parent) {
    this(container, parent, false);
  }

  public void startKeyValuePair() {
    currentChildIndex = container.getMutator().add(idx());
    for (FieldWriter w : fields.values()) {
      w.setPosition(currentChildIndex);
    }
  }

  public void endKeyValuePair() {
    checkStarted();
    // noop
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
    assert !rowStarted : "Row should not be started";
//    currentRow++; // todo: currentRow = idx();
//    length = 0;
//    rowStarted = true;
//    container.getMutator().startNewValue(idx());

    rowStarted = true;

//    final TrueMapHolder h = new TrueMapHolder();
//    final TrueMapVector map = container;
//    final TrueMapVector.Mutator mutator = container.getMutator();

    // Make sure that the current vector can support the end position of this list.
    if (container.getValueCapacity() <= idx()) {
      container.getMutator().setValueCount(idx() + 1);
    }

    TrueMapHolder h = new TrueMapHolder();
    container.getAccessor().get(idx(), h);
    if (h.start >= h.end) {
      container.getMutator().startNewValue(idx());
    }
//    currentChildIndex = container.getMutator().add(idx());
//    for (final FieldWriter w : fields.values()) {
//      w.setPosition(currentChildIndex);
//    }

//    setPosition(idx() + 1); // todo: mine!
  }

  @Override
  public void end() {
    checkStarted();

//    int offset = container.getInnerOffset(currentRow); // todo: this may be not true in a case when currentRow is
//    rowStarted = false;
//    int currentOffset = length;
//    currentOffset += offset;
////    } // todo: decide if this should be done for currentRow + 1 or not
//    container.setInnerOffset(currentRow + 1, currentOffset);

//    setPosition(idx() + 1);
    rowStarted = false;
  }

  @Deprecated
  public void put(int outputIndex, Object key, Object value) { // todo: outputIndex?
    checkStarted();

//    int index = getPosition();
    // todo: change this?
    // todo: this is wrong
//    setValue(container.getKeys(), key, index);
//    setValue(container.getValues(), value, index);
//    length++;

//    getKeyWriter().write(key);
  }

//  @Deprecated
//  private void setValue(ValueVector vector, Object value, int index) {
//    if (vector instanceof NullableIntVector) { // todo: not instanceof but type?
//      ((NullableIntVector) vector).getMutator().setSafe(index, (int) value);
//    } else if (vector instanceof NullableVarCharVector) {
//      byte[] bytes = (byte[]) value;
//      ((NullableVarCharVector) vector).getMutator().setSafe(index, bytes, 0, bytes.length);
//    }
//  }

  // todo: probably better way is to 'put' values externally (given there is access to getKeyWriter() and getValueWriter())
  @Deprecated
  public void put(int outputIndex, ValueHolder keyHolder, ValueHolder valueHolder) { // todo: outputIndex?
    checkStarted();

//    int index = getPosition();
//    getKeyWriter().setPosition(index);
//    getValueWriter().setPosition(index);

//    getKeyWriter().write(keyHolder);
//    getValueWriter().write(valueHolder);
  }

  // todo: remove
//  @Override
//  public void setPosition(int index) {
//    super.setPosition(index);
//  }

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
