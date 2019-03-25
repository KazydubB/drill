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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.memory.AllocationManager;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector.MapSingleCopier;
import org.apache.drill.exec.vector.complex.impl.AbstractFieldReader;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;
// import org.apache.drill.exec.vector.complex.impl.SingleTrueMapReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Ordering;
import org.apache.drill.shaded.guava.com.google.common.primitives.Ints;

public class TrueMapVector extends AbstractContainerVector {

  public final static MajorType TYPE = Types.required(MinorType.TRUE_MAP); // todo: change!

  public static final String FIELD_KEY = "key";
  public static final String FIELD_VALUE  = "value";

  // todo: consider fixed 'name's for vectors (key and value)
  private ValueVector keys;
  private ValueVector values;

  public long[] offsets;
  public long[] lengths;
  // the number of children slots used
  public int childCount;

  private final SingleTrueMapReaderImpl reader = new SingleTrueMapReaderImpl(TrueMapVector.this);

  //private final SingleMapReaderImpl reader = new SingleMapReaderImpl(TrueMapVector.this);
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  private int valueCount;

  /*public TrueMapVector(String path, BufferAllocator allocator, CallBack callBack) {
    this(MaterializedField.create(path, TYPE), allocator, callBack);
  }*/

  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this(field, allocator, callBack, /*null, null, */null, null);
  }

  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack, /*ValueVector keys, ValueVector values, */MajorType keyType, MajorType valueType) {
    super(field, allocator, callBack);
    /*this.keys = keys;
    this.values = values;*/
    keys = BasicTypeHelper.getNewVector(FIELD_KEY, allocator, keyType, callBack);
    values = BasicTypeHelper.getNewVector(FIELD_VALUE, allocator, valueType, callBack);
  }

  @Override
  public FieldReader getReader() {
    return reader; // reader;
  }

  transient private MapTransferPair ephPair;
  transient private MapSingleCopier ephPair2;

  public void copyFromSafe(int fromIndex, int thisIndex, MapVector from) {
    /*if (ephPair == null || ephPair.from != from) {
      ephPair = (MapTransferPair) from.makeTransferPair(this);
    }*/
    ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  public void copyFromSafe(int fromSubIndex, int thisIndex, RepeatedMapVector from) {
    if (ephPair2 == null || ephPair2.from != from) {
      // ephPair2 = from.makeSingularCopier(this);
    }
    ephPair2.copySafe(fromSubIndex, thisIndex);
  }

  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    copyFromSafe(fromIndex, toIndex, (MapVector) from);
  }

  @Override
  protected boolean supportsDirectRead() { return true; }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    for (final ValueVector v : this) {
      v.setInitialCapacity(numRecords);
    }
  }

  @Override
  public int getBufferSize() {
    if (valueCount == 0 || size() == 0) {
      return 0;
    }
    long buffer = 0;
    for (final ValueVector v : this) {
      buffer += v.getBufferSize();
    }

    return (int) buffer;
  }

  @Override
  public int getAllocatedSize() {
    int size = 0;
    for (final ValueVector v : this) {
      size += v.getAllocatedSize();
    }
    return size;
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    long bufferSize = 0;
    for (final ValueVector v : this) {
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return (int) bufferSize;
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    final List<DrillBuf> buffers = new ArrayList<>();

    // todo: this should be changed!
    clearVector(keys, buffers, clear);
    clearVector(values, buffers, clear);

    return buffers.toArray(new DrillBuf[buffers.size()]);
  }

  private void clearVector(ValueVector vector, List<DrillBuf> buffers, boolean clear) {
    for (ValueVector v : vector) {
      for (DrillBuf buf : v.getBuffers(false)) {
        buffers.add(buf);
        if (clear) {
          buf.retain(1);
        }
      }
      if (clear) {
        v.clear();
      }
    }
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
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

  protected static class MapTransferPair implements TransferPair{
    private final TransferPair[] pairs;
    private final TrueMapVector from;
    private final TrueMapVector to;

    public MapTransferPair(TrueMapVector from, String path, BufferAllocator allocator) {
      // this(from, new TrueMapVector(MaterializedField.create(path, TYPE), allocator, new SchemaChangeCallBack()), false);
      this(from, null, false);
    }

    public MapTransferPair(TrueMapVector from, TrueMapVector to) {
      this(from, to, true);
    }

    protected MapTransferPair(TrueMapVector from, TrueMapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];
      this.to.ephPair = null;
      this.to.ephPair2 = null;

      int i = 0;
      ValueVector vector;
      for (String child:from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        //DRILL-1872: we add the child fields for the vector, looking up the field by name. For a map vector,
        // the child fields may be nested fields of the top level child. For example if the structure
        // of a child field is oa.oab.oabc then we add oa, then add oab to oa then oabc to oab.
        // But the children member of a Materialized field is a HashSet. If the fields are added in the
        // children HashSet, and the hashCode of the Materialized field includes the hash code of the
        // children, the hashCode value of oa changes *after* the field has been added to the HashSet.
        // (This is similar to what happens in ScanBatch where the children cannot be added till they are
        // read). To take care of this, we ensure that the hashCode of the MaterializedField does not
        // include the hashCode of the children but is based only on MaterializedField$key.
        final ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        if (allocate && to.size() != preSize) {
          newVector.allocateNew();
        }
        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }

    @Override
    public void transfer() {
      for (final TransferPair p : pairs) {
        p.transfer();
      }
      to.valueCount = from.valueCount;
      from.clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      for (TransferPair p : pairs) {
        p.copyValueSafe(from, to);
      }
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      for (TransferPair p : pairs) {
        p.splitAndTransfer(startIndex, length);
      }
      to.getMutator().setValueCount(length);
    }
  }

  @Override
  public int getValueCapacity() {
    if (size() == 0) {
      return 0;
    }

    final Ordering<ValueVector> natural = new Ordering<ValueVector>() {
      @Override
      public int compare(@Nullable ValueVector left, @Nullable ValueVector right) {
        return Ints.compare(
            Preconditions.checkNotNull(left).getValueCapacity(),
            Preconditions.checkNotNull(right).getValueCapacity()
        );
      }
    };

    // return natural.min(getChildren()).getValueCapacity();
    return natural.min(keys).getValueCapacity();
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buf) {
    final List<SerializedField> fields = metadata.getChildList(); // todo: childlist should contain key and value as children
    valueCount = metadata.getValueCount();

    int bufOffset = 0;
    for (final SerializedField child : fields) {
      final MaterializedField fieldDef = MaterializedField.create(child);

      ValueVector vector = getChild(fieldDef.getName());
      if (vector == null) {
        // if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, allocator);
        putChild(fieldDef.getName(), vector); // todo: what to do?
      }
      if (child.getValueCount() == 0) {
        vector.clear();
      } else {
        vector.load(child, buf.slice(bufOffset, child.getBufferLength()));
      }
      bufOffset += child.getBufferLength();
    }

    // We should have consumed all bytes written into the buffer
    // during deserialization.

    assert bufOffset == buf.writerIndex();
  }

  /**
   * Inserts the vector with the given name if it does not exist else replaces it with the new value.
   *
   * Note that this method does not enforce any vector type check nor throws a schema change exception.
   */
  public void putChild(String name, ValueVector vector) {
    putVector(name, vector);
    field.addChild(vector.getField());
  }

  /**
   * Inserts the input vector into the map if it does not exist, replaces if it exists already
   * @param name  field name
   * @param vector  vector to be inserted
   */
  protected void putVector(String name, ValueVector vector) {
    Preconditions.checkNotNull(name, "field name cannot be null");
    /*ValueVector old = vectors.put(
        Preconditions.checkNotNull(name, "field name cannot be null").toLowerCase(),
        Preconditions.checkNotNull(vector, "vector cannot be null")
    );*/
    // ValueVector old = null;
    if (FIELD_KEY.equals(name)) {
      // old = keys;
      keys = vector;
    } else if (FIELD_VALUE.equals(name)) {
      // old = values;
      values = vector;
    } else {
      logger.warn("Unknown field name '{}' put into {}", name, getClass().getName());
    }
    /*if (old != null && old != vector) {
      logger.debug("Field [{}] mutated from [{}] to [{}]", name, old.getClass().getSimpleName(),
          vector.getClass().getSimpleName());
    }*/
  }

  @Override
  public SerializedField getMetadata() {
    SerializedField.Builder b = getField()
        .getAsBuilder()
        .setBufferLength(getBufferSize())
        .setValueCount(valueCount);


    for(ValueVector v : getChildren()) {
      b.addChild(v.getMetadata());
    }
    return b.build();
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public class Accessor extends BaseValueVector.BaseAccessor {

    @Override
    public Object getObject(int index) {
      Map<Object, Object> result = new JsonStringHashMap<>(); // todo: change (probably to LinkedHashMap)
      // for (String child:getChildFieldNames()) {
        // ValueVector v = getChild(child);
        // TODO(DRILL-4001):  Resolve this hack:
        // The index/value count check in the following if statement is a hack
        // to work around the current fact that RecordBatchLoader.load and
        // MapVector.load leave child vectors with a length of zero (as opposed
        // to matching the lengths of siblings and the parent map vector)
        // because they don't remove (or set the lengths of) vectors from
        // previous batches that aren't in the current batch.
        int offset = (int) offsets[index];
        int length = (int) lengths[index];
        for (int i = 0; i < length; i++) {
          // if (v != null && index < v.getAccessor().getValueCount()) {
          int valIndex = i + offset;
          Object keyVal = keys.getAccessor().getObject(valIndex);
          if (keyVal != null) {
            result.put(keyVal, values.getAccessor().getObject(valIndex));
          }
        }
        /*if (v != null && index < v.getAccessor().getValueCount()) {
          Object value = v.getAccessor().getObject(index);
          if (value != null) {
            vv.put(child, value);
          }
        }*/
      // }
      return result;
    }

    public void get(int index, ComplexHolder holder) {
      // reader.setPosition(index);
      // holder.reader = reader;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }
  }

  /*public ValueVector getVectorById(int id) {
    return getChildByOrdinal(id);
  }*/

  /**
   * Set the value count for the map without setting the counts for the contained
   * vectors. Use this only when the values of the contained vectors are set
   * elsewhere in the code.
   *
   * @param valueCount number of items in the map
   */

  public void setMapValueCount(int valueCount) {
    this.valueCount = valueCount;
  }

  public class Mutator extends BaseValueVector.BaseMutator {

    @Override
    public void setValueCount(int valueCount) {
      for (final ValueVector v : getChildren()) {
        v.getMutator().setValueCount(valueCount);
      }
      //keys.getMutator().setValueCount(valueCount);
      //values.getMutator().setValueCount(valueCount);
      setMapValueCount(valueCount); // todo: leave only this line?
    }

    @Override
    public void reset() { }

    @Override
    public void generateTestData(int values) { }
  }

  @Override
  public void clear() {
    for (ValueVector v : getChildren()) {
      v.clear();
    }
    valueCount = 0;
  }

  @Override
  public void close() {
    final Collection<ValueVector> vectors = getChildren();
    for (ValueVector v : vectors) {
      v.close();
    }
    vectors.clear();
    valueCount = 0;

    super.close();
  }

  private List<ValueVector> getChildren() {
    return Arrays.asList(keys, values);
  }

  @Override
  public void toNullable(ValueVector nullableVector) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void exchange(ValueVector other) {
    // super.exchange(other); // todo: revise
    TrueMapVector otherMap = (TrueMapVector) other;
    int temp = otherMap.valueCount;
    otherMap.valueCount = valueCount;
    valueCount = temp;
  }

  public void setKeys(ValueVector keys) {
    this.keys = keys;
  }

  public void setValues(ValueVector values) {
    this.values = values;
  }

  @Override
  public VectorWithOrdinal getChildVectorWithOrdinal(String name) {
    if (name.equals(FIELD_KEY)) {
      return new VectorWithOrdinal(keys, 0);
    } else if (name.equals(FIELD_VALUE)) {
      return new VectorWithOrdinal(values, 0);
    } else {
      logger.warn("Field with name '{}' was not found.");
      return null;
    }
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Arrays.asList(keys, values).iterator();
  }

/*@Override
  public VectorWithOrdinal getByKey(Object key) {
    return null;
  }*/

// todo: revise
  @Override
  public void allocateNew() throws OutOfMemoryException {
    super.allocateNew();
  }

  @Override
  public BufferAllocator getAllocator() {
    return super.getAllocator();
  }

  @Override
  public MaterializedField getField() {
    return super.getField();
  }

  /*@Override
  public ValueVector getChild(String name) {
    return super.getChild(name);
  }*/

  @Override
  protected Collection<String> getChildFieldNames() {
    return super.getChildFieldNames();
  }

  @Override
  protected <T extends ValueVector> T typeify(ValueVector v, Class<T> clazz) {
    return super.typeify(v, clazz);
  }

  @Override
  MajorType getLastPathType() {
    return super.getLastPathType();
  }

  @Override
  public int size() {
    return 0;
  }

  // todo: from AbstractMapVector
  public <T extends ValueVector> T addOrGet(String name, TypeProtos.MajorType type, Class<T> clazz) {
    // todo:
    if (!name.toLowerCase().equals(FIELD_KEY) && !name.toLowerCase().equals(FIELD_VALUE)) {
      logger.warn("No field {} in True map!", name);
      return null;
    }
    final ValueVector existing = getChild(name);
    boolean create = false;
    if (existing == null) {
      create = true;
    } else if (clazz.isAssignableFrom(existing.getClass())) {
      return (T) existing;
    } else if (nullFilled(existing)) {
      existing.clear();
      // Since it's removing old vector and adding new one based on new type, it should do same for Materialized field,
      // Otherwise there will be duplicate of same field with same name but different type.
      field.removeChild(existing.getField());
      create = true;
    }
    if (create) {
      final T vector = (T) BasicTypeHelper.getNewVector(name, allocator, type, callBack);
      putChild(name, vector);
      if (callBack != null) {
        callBack.doWork();
      }
      return vector;
    }
    final String message = "Drill does not support schema change yet. Existing[%s] and desired[%s] vector types mismatch";
    throw new IllegalStateException(String.format(message, existing.getClass().getSimpleName(), clazz.getSimpleName()));
  }

  private boolean nullFilled(ValueVector vector) {
    for (int r = 0; r < vector.getAccessor().getValueCount(); r++) {
      if (! vector.getAccessor().isNull(r)) {
        return false;
      }
    }
    return true;
  }

  // todo: from AbstractMapVector
  @Override
  public <T extends ValueVector> T getChild(String name, Class<T> clazz) {
    // ValueVector v = vectors.get(name.toLowerCase());
    ValueVector v = null;
    if (FIELD_KEY.equals(name.toLowerCase())) {
      v = keys;
    } else if (FIELD_KEY.equals(name.toLowerCase())) {
      v = values;
    }
    if (v == null) {
      return null;
    }
    return typeify(v, clazz);
  }

  @Override
  public VectorWithOrdinal getByKey(Object key) {
    return super.getByKey(key);
  }

  // from AbstractMapVector
  @Override
  public boolean allocateNewSafe() {
    /* boolean to keep track if all the memory allocation were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      for (ValueVector v : Arrays.asList(keys, values)) {
        if (!v.allocateNewSafe()) {
          return false;
        }
      }
      success = true;
    } finally {
      if (! success) {
        clear();
      }
    }
    return true;
  }

  @Override
  public void collectLedgers(Set<AllocationManager.BufferLedger> ledgers) {

  }

  // todo: from AbstractMapVector
  @Override
  public int getPayloadByteCount(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    int count = 0;

    for (ValueVector v : Arrays.asList(keys, values)) {
      count += v.getPayloadByteCount(valueCount);
    }
    return count;
  }

  // todo: or separate?
  public static class SingleTrueMapReaderImpl extends AbstractFieldReader {

    private final TrueMapVector vector;
    private FieldReader keyReader;
    private FieldReader valueReader;
    // private final Map<String, FieldReader> fields = Maps.newHashMap();

    public SingleTrueMapReaderImpl(TrueMapVector vector) {
      this.vector = vector;
    }

    private void setChildrenPosition(int index){
      // for(FieldReader r : fields.values()){
      for(FieldReader r : Arrays.asList(keyReader, valueReader)){
        r.setPosition(index);
      }
    }

    @Override
    public FieldReader reader(String name){
      FieldReader reader; //= fields.get(name);
      boolean key = false;
      if (name.toLowerCase().equals(FIELD_KEY)) {
        reader = keyReader;
        key = true;
      } else if (name.toLowerCase().equals(FIELD_VALUE)) {
        reader = valueReader;
      } else {
        logger.warn("True map does not support field {}", name);
        return null;
      }
      if (reader == null){
        ValueVector child = vector.getChild(name);
        if(child == null){
          reader = NullReader.INSTANCE;
        }else{
          reader = child.getReader();
        }
        // fields.put(name, reader);
        reader.setPosition(idx());
        if (key) {
          keyReader = reader;
        } else {
          valueReader = reader;
        }
      }
      return reader;
    }

    @Override
    public void setPosition(int index){
      super.setPosition(index);
      // for(FieldReader r : fields.values()){
      for(FieldReader r : Arrays.asList(keyReader, valueReader)) {
        r.setPosition(index);
      }
    }

    @Override
    public Object readObject() {
      return vector.getAccessor().getObject(idx());
    }

    @Override
    public boolean isSet() {
      return true;
    }

    @Override
    public MajorType getType(){
      return vector.getField().getType();
    }

    @Override
    public java.util.Iterator<String> iterator() {
      return vector.fieldNameIterator();
    }

    @Override
    public void copyAsValue(BaseWriter.MapWriter writer){
      //SingleMapWriter impl = (SingleTrueMapWriter) writer;
      //impl.container.copyFromSafe(idx(), impl.idx(), vector);
    }

    @Override
    public void copyAsField(String name, BaseWriter.MapWriter writer){
      SingleMapWriter impl = (SingleMapWriter) writer.map(name);
      // impl.container.copyFromSafe(idx(), impl.idx(), vector);
    }


  }
}
