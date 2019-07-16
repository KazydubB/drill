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
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

public class TrueMapReaderImpl extends RepeatedMapReaderImpl {

  private static final int NOT_FOUND = -1;
  // private final TrueMapVector vector;
//  @Deprecated
//  private int currentOffset;
//  @Deprecated
//  private int maxOffset;

  public TrueMapReaderImpl(TrueMapVector vector) {
    super(vector);
  }

  @Override // todo: check where this method is invoked and do changes
  public FieldReader reader(String name){
    // todo: NOTICE: before there was position set to child Readers!
    assert TrueMapVector.fieldNames.contains(name);
    FieldReader reader = super.reader(name); // todo: reimplement?
//    reader.setPosition(currentOffset < 0 ? 0 : currentOffset);
    return reader;
  }

  public int find(String key) {
    return find(new Text(key));
  }

  // todo: actually, key can be of any concrete type, since only literals are passed here?
  // todo: if key-uniqueness is not guaranteed, then return the last one for the key
  public int find(Object key/*, ValueHolder holder*/) {
    //public void read(Object key, NullableBigIntHolder holder) {
      /*MajorType holderType = BasicTypeHelper.getValueHolderType(holder); // todo: probably remove the check because it may be expansive
      MajorType valuesType = vector.values.getField().getType();
      if (!valuesType.equals(holderType)) {
        throw new IllegalArgumentException(String.format("invalid holder type '%s' for values of type '%s'", holderType, valuesType));
      }*/
    // todo: use idx() to get current reader position
    int idx; //= idx();
    // if (idx > 0) {
    idx = vector.getOffsetVector().getAccessor().get(idx() + 1);
//        int offset = idx() > 0 ? vector.getInnerOffsets().getAccessor().get(idx() - 1) : 0;
    int offset = vector.getOffsetVector().getAccessor().get(idx());
    //}
    int index = NOT_FOUND;
    // for (int i = 0; i < vector.lengths.getAccessor().get(idx()); i++) {
    // todo: if key-uniqueness is not guaranteed, then return the last one for the key
    for (int i = 0; i < idx - offset; i++) {
//      if (vector.getChild(TrueMapVector.FIELD_KEY_NAME).getAccessor().getObject(offset + i).toString().equals(key)) {
//      if (vector.getChild(TrueMapVector.FIELD_KEY_NAME).getAccessor().getObject(offset + i).equals(key)) {
      // todo: probably get key value of byte[] type and compare with actual values?
      Object keyValue = vector.getChild(TrueMapVector.FIELD_KEY_NAME).getAccessor().getObject(offset + i);
      if (keyValue.equals(key)) { // todo: get toString of both keyValue and key?
        index = offset + i;
        break;
      }
    }

    return index;
  }

  public void read(String key, ValueHolder holder) {
    read(new Text(key), holder);
  }

  public void read(int key, ValueHolder holder) {
    read((Object) key, holder);
  }

  // todo: use this method in case of primitive VALUE in EvaluationVisitor
  public void read(Object key, ValueHolder holder) {
    if (isNull()) {
      return; // todo: maybe check if holder is nullable and set null explicitly
    }

    int index = find(key);
    // int prevIndex = valueReader.idx();
    // todo: decide if uncomment!
    FieldReader valueReader = super.reader(TrueMapVector.FIELD_VALUE_NAME);
    valueReader.setPosition(index);
    // todo: pass the index into the method instead of setting position to the reader
    if (index > NOT_FOUND) { // todo: actually check for inequality
      valueReader.read(holder); // todo: uncomment and add to FieldReader interface. Implement for every reader (cast to that's reader ValueHolder, perhaps)
    }
    // valueReader.setPosition(prevIndex);
  }

  // todo: probably make it similarly to primitive types above
  public void read(Object key, ComplexHolder holder) {
    int index = find(key);
    if (index == NOT_FOUND) {
      holder.isSet = 0;
      // todo: include holder.reader = valueReader?
      return;
    } else {
      holder.isSet = 1;
    }
    FieldReader valueReader = super.reader(TrueMapVector.FIELD_VALUE_NAME);
    valueReader.setPosition(index);
    // holder.isSet = valueReader.isSet() ? 1 : 0;
    holder.reader = valueReader;
  }

  // todo: remove this if needed and add another method for EvaluationVisitor
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
  public Object readObject() {
    return vector.getAccessor().getObject(idx());
  }

  @Override
  public boolean isSet() {
    return currentOffset != NO_VALUES;
  }

  // todo: remove as this duplicates the method in parent
  @Override
  public boolean next() {
    if (currentOffset < maxOffset) { // todo: also add a field which will show if SPECIFIC value (i.e. found by key) is needed only
      // todo: revisit logic, ideally it'd be great to just set index field
      setChildrenPosition(++currentOffset);
      return true;
    } else {
      currentOffset = NO_VALUES;
      return false;
    }
  }

//  protected void setChildrenPosition1(int index) {
//    for (FieldReader child : fields.values()) {
//      child.index = index;
//    }
//  }

  @Override
  public TypeProtos.MajorType getType(){
    return vector.getField().getType();
  }

//    @Override
//    public java.util.Iterator<String> iterator() {
//      return vector.fieldNameIterator();
//    }

//    @Override
//    public void copyAsValue(BaseWriter.MapWriter writer){ // todo: implement this and copyAsValue(...)
//      TrueMapWriter impl = (TrueMapWriter) writer;
//      throw new AssertionError("TrueMapVector#copyAsValue(BaseWriter.MapWriter) is not implemented");
//      // impl.container.copyFromSafe(idx(), impl.idx(), vector);
//    }
//
//    @Override
//    public void copyAsField(String name, BaseWriter.MapWriter writer){
//      SingleMapWriter impl = (SingleMapWriter) writer.map(name);
//      throw new AssertionError("TrueMapVector#copyAsField(String, BaseWriter.MapWriter) is not implemented");
//      // impl.container.copyFromSafe(idx(), impl.idx(), vector);
//    }

//  @Override // todo: see UnionListReader
//  public boolean next() {
//    if (currentOffset + 1 < maxOffset) { // todo: this can be wrong!
//      vector.getReader().setPosition(++currentOffset);
//      return true;
//    } else {
//      return false;
//    }
//    // return vector.innerVector.next(); // todO: why no such method?
//  }
  // todo: use this?
//  @Override
//  public void copyAsValue(TrueMapWriter writer) {
//    ComplexCopier.copy(this, writer);
//  }

  @Override
  public void copyAsValue(TrueMapWriter writer) {
    if (isNull()) {
      return;
    }
    // todo: overload method?
//    writer.container.getChild(TrueMapVector.FIELD_VALUE_NAME).copyEntry(writer.idx(), (TrueMapVector) vector.getChild(TrueMapVector.FIELD_VALUE_NAME), idx());
//    ComplexCopier.copy(reader(TrueMapVector.FIELD_VALUE_NAME), writer);
    ComplexCopier.copy(this, writer);
  }

//  @Override
//  public void copyAsValue(IntWriter writer) {
//    copyAsValue(writer);
//  }

  public void copyAsValueSingle(TrueMapWriter writer) {
    if (isNull()) {
      return;
    }
//    writer.container.copyFromSafe(currentOffset, writer.idx(), vector);
    ComplexCopier.copy(this, writer);
  }

  /**
   * @deprecated use #copyAsValue instead
   * @param writer
   */
  @Deprecated
  @Override
  public void copySingleValue(FieldWriter writer) {
    ComplexCopier.copy(reader(TrueMapVector.FIELD_VALUE_NAME), writer);
  }
}