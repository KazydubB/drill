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
package org.apache.drill.exec.vector.complex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.TrueMapHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.SingleTrueMapReaderImpl;

public final class TrueMapVector extends RepeatedMapVector {

  public final static MajorType TYPE = Types.optional(MinorType.TRUEMAP);

  public static final String FIELD_KEY_NAME = "key";
  public static final String FIELD_VALUE_NAME = "value";
  public static final List<String> fieldNames = Collections.unmodifiableList(Arrays.asList(FIELD_KEY_NAME, FIELD_VALUE_NAME));

  private static final List<MinorType> supportedKeyTypes = Collections.unmodifiableList(Arrays.asList(MinorType.INT, MinorType.VARCHAR, MinorType.BIGINT));
  private static final List<MinorType> complexTypes = Collections.unmodifiableList(Arrays.asList(MinorType.MAP, MinorType.TRUEMAP, MinorType.LIST, MinorType.UNION));
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TrueMapVector.class);

  private MajorType keyType;
  private MajorType valueType;

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  private final SingleTrueMapReaderImpl reader;

  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    super(field.clone(), allocator, callBack);
    reader = new SingleTrueMapReaderImpl(TrueMapVector.this);
  }

  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack, MajorType keyType, MajorType valueType) {
    this(field.clone(), allocator, callBack);
    setKeyValueTypes(keyType, valueType);
  }

  @Override
  public SingleTrueMapReaderImpl getReader() {
    return reader;
  }

  @Override
  protected Collection<String> getChildFieldNames() {
    return fieldNames;
  }

  public void transferTo(TrueMapVector target) {
    super.makeTransferPair(target);
    target.setKeyValueTypes(keyType, valueType);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) { // todo: see this!
    return new MapTransferPair(this, getField().getName(), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new MapTransferPair(this, (TrueMapVector) to);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new MapTransferPair(this, ref, allocator);
  }

  private static class MapTransferPair extends RepeatedMapVector.RepeatedMapTransferPair {

    MapTransferPair(TrueMapVector from, String path, BufferAllocator allocator) {
      this(from, getNewVector(path, from, allocator), false);
    }

    MapTransferPair(TrueMapVector from, TrueMapVector to) {
      this(from, to, true);
    }

    MapTransferPair(TrueMapVector from, TrueMapVector to, boolean allocate) {
      super(from, to, allocate);
      to.keyType = from.keyType;
      to.valueType = from.valueType;
    }

    private static TrueMapVector getNewVector(String path, TrueMapVector from, BufferAllocator allocator) {
      MaterializedField field = MaterializedField.create(path, from.getField().getType());
      return new TrueMapVector(field, allocator, new SchemaChangeCallBack(), from.getKeyType(), from.getValueType());
    }
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public void putChild(String name, ValueVector vector) {
    super.putChild(name, vector);
    MajorType fieldType = vector.getField().getType();
    switch (name) {
      case FIELD_KEY_NAME:
        checkTypes(keyType, fieldType, FIELD_KEY_NAME);
        keyType = fieldType;
        break;
      case FIELD_VALUE_NAME:
        checkTypes(valueType, fieldType, FIELD_VALUE_NAME);
        valueType = fieldType;
        break;
      default:
        throw new DrillRuntimeException(String.format("Unknown field %s is added to TRUEMAP vector", name));
    }
  }

  private void checkTypes(MajorType type, MajorType newType, String fieldName) {
    assert type == null || newType.equals(type)
        : String.format("Type mismatch for %s field in TRUEMAP: expected '%s' but found '%s'", fieldName, type, newType);
  }

  @Override
  public ValueVector getChild(String name) {
    assert fieldNames.contains(name) : "TrueMapVector has 'key' and 'value' ValueVectors only";
    return super.getChild(name);
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public class Accessor extends RepeatedMapVector.RepeatedMapAccessor {

    @Override
    public Object getObject(int index) {
      int start = offsets.getAccessor().get(index);
      int end = offsets.getAccessor().get(index + 1);

      ValueVector keys = getChild(FIELD_KEY_NAME);
      ValueVector values = getChild(FIELD_VALUE_NAME);

      Map<Object, Object> result = new JsonStringHashMap<>();
      for (int i = start; i < end; i++) {
        Object key = keys.getAccessor().getObject(i);
        Object value = values.getAccessor().getObject(i);
        result.put(key, value);
      }
      return result;
    }

    public void get(int index, TrueMapHolder holder) {
      int valueCapacity = getValueCapacity();
      assert index < valueCapacity :
          String.format("Attempted to access index %d when value capacity is %d", index, valueCapacity);

      holder.vector = TrueMapVector.this;
      holder.reader = reader;
      holder.start = getOffsetVector().getAccessor().get(index);
      holder.end =  getOffsetVector().getAccessor().get(index + 1);
    }
  }

  public class Mutator extends RepeatedMapVector.Mutator {
  }

  @Override
  public void exchange(ValueVector other) {
    TrueMapVector map = (TrueMapVector) other;
    assert this.keyType == map.keyType : "Cannot exchange TrueMapVector with different key types";
    assert this.valueType == map.valueType : "Cannot exchange TrueMapVector with different value types";
    super.exchange(other);
  }

  @Override
  public VectorWithOrdinal getChildVectorWithOrdinal(String name) {
    assert fieldNames.contains(name) : "Message goes here";
    ValueVector vector = getChild(name);
    switch (name) {
      case FIELD_KEY_NAME:
        return new VectorWithOrdinal(vector, 0);
      case FIELD_VALUE_NAME:
        return new VectorWithOrdinal(vector, 1);
      default:
        logger.warn("Field with name '{}' is not present in map vector.", name);
        return null;
    }
  }

  @Override
  MajorType getLastPathType() {
    return getValues().getField().getType();
  }

  @Override
  public <T extends ValueVector> T getChild(String name, Class<T> clazz) {
    assert fieldNames.contains(name) : "No such field in TrueMapVector: " + name;
    return super.getChild(name, clazz);
  }

  public ValueVector getKeys() {
    return getChild(FIELD_KEY_NAME);
  }

  public ValueVector getValues() {
    return getChild(FIELD_VALUE_NAME);
  }

  public MajorType getKeyType() {
    return keyType;
  }

  public MajorType getValueType() {
    return valueType;
  }

  private void setKeyValueTypes(MajorType keyType, MajorType valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
  }
}
