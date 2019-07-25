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

import org.apache.drill.exec.expr.holders.RepeatedTrueMapHolder;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.RepeatedTrueMapVector;
import org.apache.drill.exec.vector.complex.TrueMapVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;
import org.apache.drill.exec.vector.complex.writer.IntWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

// todo: remove!
@Deprecated
class RepeatedTrueMapWriter1 extends AbstractFieldWriter implements BaseWriter.TrueMapWriter {

  final RepeatedTrueMapVector container;

  private final SingleTrueMapWriter trueMapWriter;
  private int currentChildIndex;

  RepeatedTrueMapWriter1(RepeatedTrueMapVector container, FieldWriter parent) {
    super(parent);
    this.container = Preconditions.checkNotNull(container, "Container cannot be null!");
    this.trueMapWriter = new SingleTrueMapWriter((TrueMapVector) container.getDataVector(), this);
  }

  @Override
  public void allocate() {
    container.allocateNew();
  }

  @Override
  public void clear() {
    container.clear();
  }

  @Override
  public void close() {
    clear();
    container.close();
  }

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  public void setValueCount(int count){
    container.getMutator().setValueCount(count);
  }

  @Override
  public void startList() {
    // make sure that the current vector can support the end position of this list.
    if (container.getValueCapacity() <= idx()) {
      container.getMutator().setValueCount(idx() + 1);
    }

    // update the repeated vector to state that there is current+1 objects.
    final RepeatedTrueMapHolder h = new RepeatedTrueMapHolder();
    container.getAccessor().get(idx(), h);
    if (h.start >= h.end) {
      container.getMutator().startNewValue(idx());
    }
    currentChildIndex = container.getOffsetVector().getAccessor().get(idx());
  }

  @Override
  public void endList() {
    // noop, we initialize state at start rather than end.
  }

  @Override
  public MaterializedField getField() {
    return container.getField();
  }

  @Override
  public BigIntWriter bigInt(String name) {
    FieldWriter writer = (FieldWriter) trueMapWriter.bigInt(name);
    writer.setPosition(currentChildIndex);
    return writer;
  }

  @Override
  public IntWriter integer(String name) {
    FieldWriter writer = (FieldWriter) trueMapWriter.integer(name);
    writer.setPosition(currentChildIndex);
    return writer;
  }

  @Override
  public void start() {
    currentChildIndex = container.getMutator().add(idx());
    trueMapWriter.setPosition(currentChildIndex);
    trueMapWriter.start();
  }

  @Override
  public void end() {
    trueMapWriter.end();
  }

  @Override
  public void startKeyValuePair() {
    trueMapWriter.startKeyValuePair();
  }

  @Override
  public void endKeyValuePair() {
    trueMapWriter.endKeyValuePair();
  }
}
