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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.memory.AllocationManager;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector.MapSingleCopier;
import org.apache.drill.exec.vector.complex.impl.AbstractFieldReader;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;
import org.apache.drill.exec.vector.complex.impl.TrueMapWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Ordering;
import org.apache.drill.shaded.guava.com.google.common.primitives.Ints;

public class TrueMapVector extends AbstractContainerVector {

  public final static MajorType TYPE = Types.required(MinorType.TRUEMAP); // todo: change!
  private final static List<MinorType> supportedKeyTypes = Collections.unmodifiableList(Arrays.asList(MinorType.INT, MinorType.VARCHAR, MinorType.BIGINT));

  public static final String FIELD_KEY_NAME = "key";
  public static final String FIELD_VALUE_NAME = "value";
  public static final String FIELD_OFFSET_NAME = "$offset$";
  public static final String FIELD_LENGTH_NAME = "$length$"; // todo: remove if unneeded

  // todo: make sure it's OK to have static field for the purpose
  private static final MaterializedField OFFSETS_FIELD = MaterializedField.create(FIELD_OFFSET_NAME, Types.required(TypeProtos.MinorType.UINT4));
  private static final MaterializedField LENGTHS_FIELD = MaterializedField.create(FIELD_LENGTH_NAME, Types.required(TypeProtos.MinorType.UINT4));

  // todo: consider fixed 'name's for vectors (key and value) // todo: make final
  private ValueVector keys;
  private ValueVector values; // private final List<ValueVector> children; // todo: uncomment
  private UInt4Vector lengths; // todo: make vectors final fields? // todo: rename to offsets!
  private UInt4Vector offsets; // todo: remove lengths vector and use offsets only!

  private final List<ValueVector> children;
  // todo: remove the types?
  private final MajorType keyType;
  private final MajorType valueType;

  private final SingleTrueMapReaderImpl reader = new SingleTrueMapReaderImpl(TrueMapVector.this);

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  private int valueCount;

  @Deprecated // todo: remove the method
  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this(field, allocator, callBack, null, null);
  }

  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack, MajorType keyType, MajorType valueType) {
    super(field, allocator, callBack);
    boolean keyTypeSupported =
        keyType.getMode() != TypeProtos.DataMode.REPEATED && supportedKeyTypes.contains(keyType.getMinorType());
    Preconditions.checkArgument(keyTypeSupported, "Unsupported key type in TRUEMAP: " + keyType);
    this.keyType = keyType;
    this.valueType = valueType;
    keys = BasicTypeHelper.getNewVector(FIELD_KEY_NAME, allocator, keyType, callBack);
    values = BasicTypeHelper.getNewVector(FIELD_VALUE_NAME, allocator, valueType, callBack);
    offsets = new UInt4Vector(OFFSETS_FIELD, allocator);
    lengths = new UInt4Vector(LENGTHS_FIELD, allocator);
    children = Collections.unmodifiableList(Arrays.asList(keys, values, lengths, offsets));
    for (ValueVector child : children) { // todo: ListVector does not add children to field, for example. Maybe discard it here also?
      field.addChild(child.getField());
    }
  }

  @Override
  public SingleTrueMapReaderImpl getReader() {
    return reader;
  }

  transient private MapTransferPair ephPair;
  transient private MapSingleCopier ephPair2;

  public void copyFromSafe(int fromIndex, int thisIndex, MapVector from) {
    /*if (ephPair == null || ephPair.from != from) {
      ephPair = (MapTransferPair) from.makeTransferPair(this);
    }*/
    ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  public void copyFromSafe(int fromSubIndex, int thisIndex, RepeatedMapVector from) { // todo: not sure what is the purpose
    if (ephPair2 == null || ephPair2.from != from) {
      // ephPair2 = from.makeSingularCopier(this);
    }
    // ephPair2.copySafe(fromSubIndex, thisIndex);
  }
  // todo: looks like this one is used when query contains LIMIT
  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    System.out.println("No idea!");// copyFromSafe(fromIndex, toIndex, (MapVector) from);
  }

  @Override
  protected boolean supportsDirectRead() { return true; }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    for (ValueVector v : children) { // todo: was ... : this
      v.setInitialCapacity(numRecords);
    }
  }

  @Override
  public int getBufferSize() {
    if (valueCount == 0 /*|| size() == 0*/) {
      return 0;
    }
    long buffer = 0; // todo: check if valueCounts are set in children if it affects the result actually
    for (ValueVector v : children) { // todo: was ... : this
      buffer += v.getBufferSize();
    }

    return (int) buffer;
  }

  @Override
  public int getAllocatedSize() {
    int size = 0;
    for (ValueVector v : children) { // todo: was ... : this
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
    for (ValueVector v : children) { // todo: was ... : this
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return (int) bufferSize;
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    final List<DrillBuf> buffers = new ArrayList<>();

    for (ValueVector vector : children) {
      getBuffers(vector, buffers, clear);
    }

    return buffers.toArray(new DrillBuf[0]);
  }
// todo: merge with method above
  private void getBuffers(ValueVector vector, List<DrillBuf> buffers, boolean clear) {
    for (DrillBuf buf : vector.getBuffers(false)) {
      buffers.add(buf);
      if (clear) {
        buf.retain();
      }
    }
    if (clear) {
      vector.clear();
    }
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

  protected static class MapTransferPair implements TransferPair {
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
      for (String child : from.getChildFieldNames()) {
        int preSize = to.size(); // todo: this may be incorrect // todo: get back to this
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
        if (allocate /*&& to.size() != preSize*/) { // todo: uncomment? Revise!
          newVector.allocateNew(); // todo: do not allocate everytime!
        }
        pairs[i++] = vector.makeTransferPair(newVector); // todo: make sure this one is ok
      }
    }

    @Override
    public void transfer() { // todo: this one of interest
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
    /*if (size() == 0) { // todo: this can be removed as this always has 4 children
      return 0;
    }*/

    final Ordering<ValueVector> natural = new Ordering<ValueVector>() {
      @Override
      public int compare(@Nullable ValueVector left, @Nullable ValueVector right) {
        return Ints.compare(
            Preconditions.checkNotNull(left).getValueCapacity(),
            Preconditions.checkNotNull(right).getValueCapacity()
        );
      }
    };

    return natural.min(children).getValueCapacity();
    // return natural.min(keys).getValueCapacity();
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buf) { // todo: this should be called?
    final List<SerializedField> fields = metadata.getChildList(); // todo: childlist should contain key and value as children
    valueCount = metadata.getValueCount();
// todo: should include offsets vector
    int bufOffset = 0;
    for (SerializedField child : fields) {
      MaterializedField fieldDef = MaterializedField.create(child);

      ValueVector vector = getChild(fieldDef.getName());
      /*if (vector == null) { // todo: this should not be the case, should it?
        // if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, allocator);
        putChild(fieldDef.getName(), vector); // todo: what to do?
      }*/
      if (child.getValueCount() == 0) {
        vector.clear();
      } else {
        vector.load(child, buf.slice(bufOffset, child.getBufferLength())); // todo: offsets vector is not loaded here... :(
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
  @Deprecated
  public void putChild(String name, ValueVector vector) {
    putVector(name, vector);
    field.addChild(vector.getField());
  }

  /**
   * Inserts the input vector into the map if it does not exist, replaces if it exists already
   * @param name  field name
   * @param vector  vector to be inserted
   */
  @Deprecated
  protected void putVector(String name, ValueVector vector) {
    Preconditions.checkNotNull(name, "field name cannot be null");
    /*ValueVector old = vectors.put(
        Preconditions.checkNotNull(name, "field name cannot be null").toLowerCase(),
        Preconditions.checkNotNull(vector, "vector cannot be null")
    );*/
    // ValueVector old = null;
    if (FIELD_KEY_NAME.equals(name)) {
      // old = keys;
      keys = vector;
    } else if (FIELD_VALUE_NAME.equals(name)) {
      // old = values;
      values = vector;
    } else if (FIELD_OFFSET_NAME.equals(name)) {
      offsets = (UInt4Vector) vector; // todo: revise
    } else if (FIELD_LENGTH_NAME.equals(name)) {
      lengths = (UInt4Vector) vector; // todo: revise
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

    for (ValueVector v : children) {
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
      int offset = 0;
      if (index > 0) {
        offset = offsets.getAccessor().get(index - 1);
      }
      int length = lengths.getAccessor().get(index);
      for (int i = 0; i < length; i++) {
        int valIndex = offset + i;
        Object keyVal = keys.getAccessor().getObject(valIndex);
        result.put(keyVal, values.getAccessor().getObject(valIndex));
      }
      return result;
    }

    // todo: what to do?
    public void get(int index, ComplexHolder holder) {
      reader.setPosition(index);
      holder.reader = reader;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }
  }

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

  // todo: move this logic to TrueMapWriter?
  public class Mutator extends BaseValueVector.BaseMutator {
    // todo: move to TrueMapVector?
    private int currentRow = -1;
    private int length = -1; // (designates length for current row)
    private boolean rowStarted;

    public void startRow() {
      currentRow++;
      length = 0;
      rowStarted = true;
    }

    public void endRow() {
      // todo: implement if needed
      rowStarted = false;
      int currentOffset = length;
      if (currentRow > 0) {
        currentOffset += offsets.getAccessor().get(currentRow - 1);
      } // todo: decide if this should be done for currentRow + 1 or not
      offsets.getMutator().setSafe(currentRow, currentOffset);
      lengths.getMutator().setSafe(currentRow, length); // todo:
    }

    public void put(int outputIndex, Object key, Object value) { // todo: outputIndex?
      assert rowStarted : "Must start row (startRow()) before put";

      // todo: maybe move the offset setting into endRow method?
      int offsetsCapacity = offsets.getValueCapacity();
      int offset = offsetsCapacity > currentRow ? offsets.getAccessor().get(currentRow) : 0; // todo: this may be not true in a case when currentRow is
      int index = offset + length;

      setValue(keys, key, index);
      setValue(values, value, index);
      length++;
    }

    private void setValue(ValueVector vector, Object value, int index) {
      if (vector instanceof NullableIntVector) {
        ((NullableIntVector) vector).getMutator().setSafe(index, (int) value);
      } else if (vector instanceof NullableVarCharVector) {
        byte[] keyData = (byte[]) value;
        ((NullableVarCharVector) vector).getMutator().setSafe(index, keyData, 0, keyData.length);
      }
    }

    @Override
    public void setValueCount(int valueCount) { // todo: change keys and values to reflect offset as count?
      int childValueCount = valueCount > 0 ? offsets.getAccessor().get(valueCount - 1) : 0;
      keys.getMutator().setValueCount(childValueCount);
      values.getMutator().setValueCount(childValueCount);
      offsets.getMutator().setValueCount(valueCount);
      lengths.getMutator().setValueCount(valueCount);
      setMapValueCount(valueCount);
    }

    public void setNull() {

    }

    @Override
    public void reset() {
    }

    @Override
    public void generateTestData(int values) {
    }
  }

  @Override
  public void clear() { // todo: uncomment
    for (ValueVector v : children) {
      v.clear();
    }
    valueCount = 0;
  }

  @Override
  public void close() {
    // final Collection<ValueVector> vectors = getChildren();
    /*for (ValueVector v : children) {
      v.close();
    }*/
    // vectors.clear(); this seems unnecessary, is it?
    super.close();
    valueCount = 0;
  }

  @Override
  public void toNullable(ValueVector nullableVector) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void exchange(ValueVector other) {
    // super.exchange(other); // todo: revise
    // AbstractMapVector otherMap = (AbstractMapVector) other;
    TrueMapVector otherMap = (TrueMapVector) other;
    // if (vectors.size() != otherMap.vectors.size()) {
    if (valueCount != otherMap.valueCount) { // todo: what this valueCount means?
      throw new IllegalStateException("Maps have different column counts");
    }
    // for (int i = 0; i < vectors.size(); i++) {
    // for (int i = 0; i < valueCount; i++) {
      // assert vectors.getByOrdinal(i).getField().isEquivalent(otherMap.vectors.getByOrdinal(i).getField());
      // vectors.getByOrdinal(i).exchange(otherMap.vectors.getByOrdinal(i));
      /*keys.exchange(otherMap.keys); // todo: uncomment if does not work
      values.exchange(otherMap.values);
      offsets.exchange(otherMap.offsets);
      offsets.exchange(otherMap.offsets);*/
      for (int i = 0; i < children.size(); i++) { // todo: changed recently
        children.get(i).exchange(otherMap.children.get(i));
      }
    // }
    // TrueMapVector otherMap = (TrueMapVector) other;
    int temp = otherMap.valueCount;
    otherMap.valueCount = valueCount;
    valueCount = temp;
  }

  /*public void setKeys(ValueVector keys) {
    this.keys = keys;
  }

  public void setValues(ValueVector values) {
    this.values = values;
  }*/

  @Override
  public VectorWithOrdinal getChildVectorWithOrdinal(String name) { // todo: discard?
    if (name.equals(FIELD_KEY_NAME)) {
      return new VectorWithOrdinal(keys, 0);
    } else if (name.equals(FIELD_VALUE_NAME)) {
      return new VectorWithOrdinal(values, 1);
    } else {
      logger.warn("Field with name '{}' was not found.");
      return null;
    }
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return children.iterator();
  }

  @Override
  MajorType getLastPathType() { // todo: probably introduce another method
    // return super.getLastPathType(); // todo: return key type? // todo: even value type?
    return values.getField().getType();
  }

  @Override
  public int size() {
    return children.size();
  }

  // todo: from AbstractMapVector
  public <T extends ValueVector> T addOrGet(String name, TypeProtos.MajorType type, Class<T> clazz) {
    // todo:
    if (!name.equals(FIELD_KEY_NAME) && !name.equals(FIELD_VALUE_NAME) && !name.equals(FIELD_OFFSET_NAME) && !name.equals(FIELD_LENGTH_NAME)) {
      logger.warn("No field {} in True map!", name);
      return null;
    }
    final ValueVector existing = getChild(name);
    // boolean create = false;
    if (existing == null) {
      // create = true;
    } else if (clazz.isAssignableFrom(existing.getClass())) {
      return (T) existing;
    } /*else if (nullFilled(existing)) { // todo: not sure whether need this part, Probably, remove
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
    }*/
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
    if (FIELD_KEY_NAME.equals(name)) {
      v = keys;
    } else if (FIELD_VALUE_NAME.equals(name)) {
      v = values;
    } else if (FIELD_OFFSET_NAME.equals(name)) {
      v = offsets;
    } else if (FIELD_LENGTH_NAME.equals(name)) {
      v = lengths;
    }
    if (v == null) { // todo: safe to do else
      return null;
    }
    return typeify(v, clazz);
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
      for (ValueVector v : children) {
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
    for (ValueVector v : children) {
      count += v.getPayloadByteCount(valueCount);
    }
    return count;
  }

  // todo: or separate?
  public static class SingleTrueMapReaderImpl extends AbstractFieldReader {

    private final TrueMapVector vector;
    private FieldReader keyReader;
    private FieldReader valueReader;
    // todo: add offsetsReader?
    // private final Map<String, FieldReader> fields = Maps.newHashMap();

    public SingleTrueMapReaderImpl(TrueMapVector vector) {
      this.vector = vector;
      // keyReader = vector.keys.getReader();
    }

    private void setChildrenPosition(int index){
      // for(FieldReader r : fields.values()){
      for(FieldReader r : Arrays.asList(keyReader, valueReader)){
        r.setPosition(index);
      }
    }

    @Override // todo: check where this method is invoked and do changes
    public FieldReader reader(String name){
      FieldReader reader; //= fields.get(name);
      boolean key = false;
      if (name.toLowerCase().equals(FIELD_KEY_NAME)) {
        reader = keyReader;
        key = true;
      } else if (name.toLowerCase().equals(FIELD_VALUE_NAME)) {
        reader = valueReader;
      } else { // todo: add offsets reader
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
      for(FieldReader r : Arrays.asList(keyReader, valueReader)) { // todo: add offsetsReader
        r.setPosition(index);
      }
    }

    public void read(Object key, ValueHolder holder) {
    //public void read(Object key, NullableBigIntHolder holder) {
      MajorType holderType = BasicTypeHelper.getValueHolderType(holder); // todo: probably remove the check because it may be expansive
      MajorType valuesType = vector.values.getField().getType();
      if (!valuesType.equals(holderType)) {
        throw new IllegalArgumentException(String.format("invalid holder type '%s' for values of type '%s'", holderType, valuesType));
      }

      int index = -1;
      // for (int i = 0; i < vector.keys.getAccessor().getValueCount(); i++) {
      // for (int i = 0; i < vector.offsets[idx()]; i++) {
      for (int i = 0; i < vector.lengths.getAccessor().get(idx()); i++) {
        if (vector.keys.getAccessor().getObject(i).toString().equals(key)) {
          index = i;
          break;
        }
      }

      boolean found = index != -1;
      NullableIntHolder h = (NullableIntHolder) holder;
      h.isSet = found ? 1 : 0;
      if (found) {
        h.value = (int) vector.values.getAccessor().getObject(index);
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
      TrueMapWriter impl = (TrueMapWriter) writer;
      // impl.container.copyFromSafe(idx(), impl.idx(), vector);
    }

    @Override
    public void copyAsField(String name, BaseWriter.MapWriter writer){
      SingleMapWriter impl = (SingleMapWriter) writer.map(name);
      // impl.container.copyFromSafe(idx(), impl.idx(), vector);
    }
  }

  public boolean isEmpty(int index) {
    // todo: check if offsets is not empty
    return lengths.getAccessor().get(index) == 0;
  }
}
