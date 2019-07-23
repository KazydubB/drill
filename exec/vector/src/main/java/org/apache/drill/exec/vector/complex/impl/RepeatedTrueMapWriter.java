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

import com.google.common.base.Preconditions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.RepeatedTrueMapHolder;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.RepeatedTrueMapVector;
import org.apache.drill.exec.vector.complex.TrueMapVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;
import org.apache.drill.exec.vector.complex.writer.IntWriter;

public class RepeatedTrueMapWriter extends AbstractFieldWriter implements BaseWriter.TrueMapWriter {

  private final RepeatedTrueMapVector container;
  private final SingleTrueMapWriter trueMapWriter;
  private int currentChildIndex;

  RepeatedTrueMapWriter(RepeatedTrueMapVector container, FieldWriter parent) {
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

  private static final TypeProtos.MajorType BIGINT_TYPE = Types.optional(TypeProtos.MinorType.BIGINT);
  @Override
  public BigIntWriter bigInt(String name) {
//    FieldWriter writer = fields.get(name.toLowerCase());
//    if(writer == null) {
//      ValueVector vector;
//      ValueVector currentVector = container.getInnerVector().getChild(name);
////      if (unionEnabled){
////        NullableBigIntVector v = container.addOrGet(name, BIGINT_TYPE, NullableBigIntVector.class);
////        writer = new PromotableWriter(v, container);
////        vector = v;
////      } else {
//        NullableBigIntVector v = container.getInnerVector().addOrGet(name, BIGINT_TYPE, NullableBigIntVector.class);
//        writer = new NullableBigIntWriterImpl(v, this);
//        vector = v;
////      }
//      if (currentVector == null || currentVector != vector) {
//        vector.allocateNewSafe();
//      }
//      writer.setPosition(currentChildIndex);
//      fields.put(name.toLowerCase(), writer);
//    }
//    return writer;
    FieldWriter writer = (FieldWriter) trueMapWriter.bigInt(name);
    writer.setPosition(currentChildIndex);
    return writer;
  }

  private static final TypeProtos.MajorType INT_TYPE = Types.optional(TypeProtos.MinorType.INT);
  @Override
  public IntWriter integer(String name) {
//    FieldWriter writer = fields.get(name.toLowerCase());
//    if(writer == null) {
//      ValueVector vector;
//      ValueVector currentVector = container.getInnerVector().getChild(name);
////      if (unionEnabled){
////        NullableBigIntVector v = container.addOrGet(name, BIGINT_TYPE, NullableBigIntVector.class);
////        writer = new PromotableWriter(v, container);
////        vector = v;
////      } else {
//      NullableIntVector v = container.getInnerVector().addOrGet(name, INT_TYPE, NullableIntVector.class);
//      writer = new NullableIntWriterImpl(v, this);
//      vector = v;
////      }
//      if (currentVector == null || currentVector != vector) {
//        vector.allocateNewSafe();
//      }
//      writer.setPosition(currentChildIndex);
//      fields.put(name.toLowerCase(), writer);
//    }
//    return writer;
    FieldWriter writer = (FieldWriter) trueMapWriter.integer(name);
    writer.setPosition(currentChildIndex);
    return writer;
  }

  @Override
  public void start() {
//    trueMapWriter.setPosition(currentChildIndex);
//    trueMapWriter.start();
    currentChildIndex = container.getMutator().add(idx());
    trueMapWriter.setPosition(currentChildIndex);
    trueMapWriter.start();
//    for (FieldWriter w : trueMapWriter.getEntryWriters()) {
//      w.setPosition(currentChildIndex);
//    }
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

//  @Override
//  public void copyAsValue(BaseWriter.TrueMapWriter writer) {
//    if (isNull()) {
//      return;
//    }
//    ComplexCopier.copy(this, (FieldWriter) writer);
//  }
//
//  @Override
//  public void copyAsValue(BaseWriter.ListWriter writer) {
//    ComplexCopier.copy(this, (FieldWriter) writer.trueMap());
//  }
}
