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

import io.netty.buffer.DrillBuf;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.TrueMapHolder;
import org.apache.drill.exec.memory.AllocationManager;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.TrueMapReaderImpl;
// todo: remove the import?
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
// todo: extend ListVector?
public final class TrueMapVector extends RepeatedMapVector {

  @Deprecated // todo: remove it
  public final static MajorType TYPE = Types.optional(MinorType.TRUEMAP); // todo: change!

  public static final String FIELD_KEY_NAME = "key";
  public static final String FIELD_VALUE_NAME = "value";
  public static final List<String> fieldNames = Collections.unmodifiableList(Arrays.asList(FIELD_KEY_NAME, FIELD_VALUE_NAME)); // todo: rename to childNames
  public static final int NUMBER_OF_CHILDREN = 2;

  private static final List<MinorType> supportedKeyTypes = Collections.unmodifiableList(Arrays.asList(MinorType.INT, MinorType.VARCHAR, MinorType.BIGINT));
  private static final List<MinorType> complexTypes = Collections.unmodifiableList(Arrays.asList(MinorType.MAP, MinorType.TRUEMAP, MinorType.LIST, MinorType.UNION));
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TrueMapVector.class);

  private MajorType keyType;
  private MajorType valueType;

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  private final TrueMapReaderImpl reader;

  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    super(field.clone(), allocator, callBack);
    reader = new TrueMapReaderImpl(TrueMapVector.this);
  }

  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack, MajorType keyType, MajorType valueType) {
    this(field.clone(), allocator, callBack);
    setKeyValueTypes(keyType, valueType);
  }

  @Override
  public TrueMapReaderImpl getReader() {
    return reader;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    checkInitialized();
    super.setInitialCapacity(numRecords);
  }

  @Override
  public int getBufferSize() {
    checkInitialized();
    return super.getBufferSize();
  }

  @Override
  public int getAllocatedSize() {
    checkInitialized();
    return super.getAllocatedSize();
  }

  public boolean initialized() {
    return size() == NUMBER_OF_CHILDREN;
  }

  // todo: remove
  @Deprecated
  private void checkInitialized() {
//    assert initialized() : "TrueMapVector is not initialized";
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    checkInitialized();
    return super.getBufferSizeFor(valueCount);
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    checkInitialized();
    return super.getBuffers(clear);
  }

  @Override
  protected Collection<String> getChildFieldNames() {
    return fieldNames;
  }

  public void transferTo(TrueMapVector target) {
    checkInitialized();
    super.makeTransferPair(target);
    target.setKeyValueTypes(keyType, valueType);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) { // todo: see this!
    return new MapTransferPair(this, getField().getName(), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new MapTransferPair(this, (TrueMapVector) to); // todo: here?
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new MapTransferPair(this, ref, allocator);
  }

//  private static class MapTransferPair implements TransferPair { // todo: extend one for repeated map?
  private static class MapTransferPair extends RepeatedMapVector.RepeatedMapTransferPair { // todo: extend one for repeated map?
    private final TransferPair[] pairs;
    private final TrueMapVector from;
    private final TrueMapVector to;
// todo: getField()?
    MapTransferPair(TrueMapVector from, String path, BufferAllocator allocator) {
      this(from, new TrueMapVector(MaterializedField.create(path, from.getField().getType()), allocator, new SchemaChangeCallBack(), from.getKeyType(), from.getValueType()), false);
    }

    MapTransferPair(TrueMapVector from, TrueMapVector to) {
      this(from, to, true);
    }

    MapTransferPair(TrueMapVector from, TrueMapVector to, boolean allocate) {
      super(from, to, allocate);
      this.from = from;
      this.to = to;
      this.to.keyType = this.from.keyType;
      this.to.valueType = this.from.valueType;
      this.pairs = new TransferPair[from.size()];

      int i = 0;
      ValueVector vector;
      for (String child : from.getChildFieldNames()) {
        int preSize = to.size(); // todo: this may be incorrect // todo: get back to this
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        // todo: remove?
        //DRILL-1872: we add the child fields for the vector, looking up the field by name. For a map vector,
        // the child fields may be nested fields of the top level child. For example if the structure
        // of a child field is oa.oab.oabc then we add oa, then add oab to oa then oabc to oab.
        // But the children member of a Materialized field is a HashSet. If the fields are added in the
        // children HashSet, and the hashCode of the Materialized field includes the hash code of the
        // children, the hashCode value of oa changes *after* the field has been added to the HashSet.
        // (This is similar to what happens in ScanBatch where the children cannot be added till they are
        // read). To take care of this, we ensure that the hashCode of the MaterializedField does not
        // include the hashCode of the children but is based only on MaterializedField$key.
        final ValueVector newVector;
        if (vector.getField().getType().getMinorType() == MinorType.TRUEMAP) {
          TrueMapVector mapVector = (TrueMapVector) vector;
          newVector = to.addOrGet(child, mapVector.getField().getType(), mapVector.getKeyType(), mapVector.getValueType());
          // todo: handle children here?
        } else {
          newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        }
        if (allocate && to.size() != preSize) { // todo: uncomment? Revise!
          newVector.allocateNew(); // todo: do not allocate everytime!
        }
        pairs[i++] = vector.makeTransferPair(newVector); // todo: make sure this one is ok
      }
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void transfer() {
      from.offsets.transferTo(to.offsets);
      for (TransferPair p : pairs) {
        p.transfer();
      }
      from.clear();
    }

    @Override
    public void copyValueSafe(int srcIndex, int destIndex) {
      TrueMapHolder holder = new TrueMapHolder();
      from.getAccessor().get(srcIndex, holder);
      to.emptyPopulator.populate(destIndex + 1); // todo: uncomment?!
      int newIndex = to.offsets.getAccessor().get(destIndex);
      //todo: make these bulk copies
      for (int i = holder.start; i < holder.end; i++, newIndex++) {
        for (TransferPair p : pairs) {
          p.copyValueSafe(i, newIndex);
        }
      }
      to.offsets.getMutator().setSafe(destIndex + 1, newIndex);
    }

    @Override
    public void splitAndTransfer(final int groupStart, final int groups) {
      final UInt4Vector.Accessor a = from.offsets.getAccessor();
      final UInt4Vector.Mutator m = to.offsets.getMutator();

      final int startPos = a.get(groupStart);
      final int endPos = a.get(groupStart + groups);
      final int valuesToCopy = endPos - startPos;

      to.offsets.clear();
      to.offsets.allocateNew(groups + 1);

      int normalizedPos;
      for (int i = 0; i < groups + 1; i++) {
        normalizedPos = a.get(groupStart + i) - startPos;
        m.set(i, normalizedPos);
      }

      m.setValueCount(groups + 1);
      to.emptyPopulator.populate(groups);

      for (final TransferPair p : pairs) {
        p.splitAndTransfer(startPos, valuesToCopy);
      }
    }
  }

  @Override
  public int getValueCapacity() {
    checkInitialized();
    return super.getValueCapacity();
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public void putChild(String name, ValueVector vector) {
    super.putChild(name, vector);
    // todo: decide what to do with previously set key/value types if any (make check for consistency between old and new or discard old etc.)
    switch (name) {
      case FIELD_KEY_NAME:
        keyType = vector.getField().getType();
        break;
      case FIELD_VALUE_NAME:
        valueType = vector.getField().getType();
        break;
      default:
        logger.warn("Unknown field {} was added to TRUEMAP vector", name);
    }
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
      int offset = offsets.getAccessor().get(index);
      int length = offsets.getAccessor().get(index + 1) - offset;
      ValueVector keys = getChild(FIELD_KEY_NAME);
      ValueVector values = getChild(FIELD_VALUE_NAME);

      Map<Object, Object> result = new JsonStringHashMap<>();
      for (int i = 0; i < length; i++) {
        int valIndex = offset + i;
        Object key = keys.getAccessor().getObject(valIndex);
        Object value = values.getAccessor().getObject(valIndex);
        result.put(key, value);
      }
      return result;
    }

    @Override
    public boolean isNull(int index) {
      return false; // todo: redefine if Nullable
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

    // todo: see if this can be removed?
//    @Override
//    public void setValueCount(int valueCount) {
////      checkInitialized();
//      // todo: was working well
////      offsets.getMutator().setValueCount(valueCount == 0 ? 0 : valueCount + 1);
////      int childValueCount = offsets.getAccessor().get(valueCount);
////      for (final ValueVector v : getChildren()) {
////        v.getMutator().setValueCount(childValueCount);
////      }
//      super.setValueCount(valueCount);
//    }


    // todo; needs implementation?
//    @Override
//    public void exchange(ValueVector.Mutator other) {
//    }
  }

  @Override
  public void toNullable(ValueVector nullableVector) {
    throw new UnsupportedOperationException();
  }

  // todo: probably remove
  @Override
  public void exchange(ValueVector other) {
    TrueMapVector map = (TrueMapVector) other;
    assert this.keyType == map.keyType : "Cannot exchange TrueMapVector with different key types";
    assert this.valueType == map.valueType : "Cannot exchange TrueMapVector with different value types";
    super.exchange(other);
  }

  @Override
  public VectorWithOrdinal getChildVectorWithOrdinal(String name) { // todo: discard?
    assert fieldNames.contains(name) : "Message goes here";
    ValueVector vector = getChild(name);
    switch (name) {
      case FIELD_KEY_NAME:  // todo: create
        return new VectorWithOrdinal(vector, 0);
      case FIELD_VALUE_NAME:
        return new VectorWithOrdinal(vector, 1);
      default:
        logger.warn("Field with name '{}' is not present in map vector.");
        return null;
    }
  }

  @Override
  MajorType getLastPathType() { // todo: probably introduce another method
    // return super.getLastPathType(); // todo: return key type? // todo: even value type?
    return getChild(FIELD_VALUE_NAME).getField().getType(); // todo: must handle complex values... One solution may be to add a method, which gets type of value
  }

  // todo: remove this method
  @Deprecated
  public TrueMapVector addOrGet(String name, MajorType type, MajorType keyType, MajorType valueType) {
    TrueMapVector vector = super.addOrGet(name, type, TrueMapVector.class);
    vector.setKeyValueTypes(keyType, valueType);
    return vector;
  }

  @Override
  public <T extends ValueVector> T getChild(String name, Class<T> clazz) {
    assert fieldNames.contains(name) : "No such field in TrueMapVector";
    return super.getChild(name, clazz);
  }

  @Override
  public void collectLedgers(Set<AllocationManager.BufferLedger> ledgers) { // todo: what the heck?
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

  public void setKeyValueTypes(MajorType keyType, MajorType valueType) {
    boolean keyTypeSupported =
        /*keyType.getMode() == TypeProtos.DataMode.REQUIRED &&*/ supportedKeyTypes.contains(keyType.getMinorType());
    Preconditions.checkArgument(keyTypeSupported,
        "Unsupported key type in TRUEMAP: " + keyType + ". Key should be (REQUIRED?) primitive type");
    this.keyType = keyType;
    this.valueType = valueType;
  }
}
