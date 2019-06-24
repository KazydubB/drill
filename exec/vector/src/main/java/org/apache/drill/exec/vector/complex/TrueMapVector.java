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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.TrueMapHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.memory.AllocationManager;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.AbstractFieldReader;
import org.apache.drill.exec.vector.complex.impl.ComplexCopier;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;
import org.apache.drill.exec.vector.complex.impl.TrueMapWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
// todo: consider implementing RepeatedValueVector...
public class TrueMapVector extends AbstractContainerVector {

  @Deprecated // todo: remove it
  public final static MajorType TYPE = Types.optional(MinorType.TRUEMAP); // todo: change!
  private final static List<MinorType> supportedKeyTypes = Collections.unmodifiableList(Arrays.asList(MinorType.INT, MinorType.VARCHAR, MinorType.BIGINT));
  private final static List<MinorType> complexTypes = Collections.unmodifiableList(Arrays.asList(MinorType.MAP, MinorType.TRUEMAP, MinorType.LIST, MinorType.UNION));

  public static final String FIELD_INNER_REPEATED_STRUCT_NAME = "$inner_repeated_struct_name$";
  public static final String FIELD_KEY_NAME = "key";
  public static final String FIELD_VALUE_NAME = "value";
  // todo: do not use name for the field and use getOffsets()

  // private static final String FIELD_OFFSET_NAME = "$offset$";
  @Deprecated
  private static final String FIELD_OFFSET_NAME = BaseRepeatedValueVector.OFFSETS_VECTOR_NAME;
  @Deprecated
  private static final String FIELD_LENGTH_NAME = "$length$"; // todo: remove if unneeded

  // @Deprecated
  // public static final List<String> fieldNames = Collections.unmodifiableList(Arrays.asList(FIELD_KEY_NAME, FIELD_VALUE_NAME, FIELD_OFFSET_NAME, FIELD_LENGTH_NAME));
  public static final List<String> fieldNames = Collections.unmodifiableList(Arrays.asList(FIELD_KEY_NAME, FIELD_VALUE_NAME)); // todo: rename to childNames
  public static final int NUMBER_OF_CHILDREN = 2;

  // todo: make sure it's OK to have static field for the purpose
  // private static final MaterializedField OFFSETS_FIELD = MaterializedField.create(FIELD_OFFSET_NAME, Types.required(TypeProtos.MinorType.UINT4));
  // private static final MaterializedField LENGTHS_FIELD = MaterializedField.create(FIELD_LENGTH_NAME, Types.required(TypeProtos.MinorType.UINT4));

  private static final int NOT_FOUND = -1;

  // todo: add parent writer? probably not
  // todo: consider fixed 'name's for vectors (key and value) // todo: make final
  // private ValueVector keys;
  // private ValueVector values; // private final List<ValueVector> children; // todo: uncomment
  private RepeatedMapVector innerVector;
  // @Deprecated
  // private UInt4Vector lengths; // todo: make vectors final fields? // todo: rename to offsets!
  // NOTE: offsets doesn't store the first 0 offset.
  // private UInt4Vector offsets; // todo: remove lengths vector and use offsets only!
  // todo: consider bits vv for nullability...
  @Deprecated
  private TrueMapWriter writer; // todo: remove!?

//  @Deprecated
//  private /*final*/ List<ValueVector> children = Collections.emptyList(); // todo: this may be tricky
  // todo: remove the types?
  private /*final*/ MajorType keyType;
  private /*final*/ MajorType valueType;

  private final SingleTrueMapReaderImpl reader;//  = new SingleTrueMapReaderImpl(TrueMapVector.this);

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  @Deprecated
  private int valueCount; // todo: is this needed?

  // private boolean initialized; // todo: remove

  // todo: remove DEPRECATION?
  // @Deprecated // todo: remove the method
  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    super(field.clone(), allocator, callBack);

    MaterializedField innerVectorField = MaterializedField.create(FIELD_INNER_REPEATED_STRUCT_NAME, RepeatedMapVector.TYPE);
    innerVector = new RepeatedMapVector(innerVectorField, allocator, callBack);
    this.field.addChild(innerVectorField);

    reader = new SingleTrueMapReaderImpl(TrueMapVector.this);
  }

  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack, MajorType keyType, MajorType valueType) {
    super(field.clone(), allocator, callBack);
    boolean keyTypeSupported =
        keyType.getMode() == TypeProtos.DataMode.REQUIRED && supportedKeyTypes.contains(keyType.getMinorType());
    Preconditions.checkArgument(keyTypeSupported, "Unsupported key type in TRUEMAP: " + keyType + ". Key should be REQUIRED primitive type");
    this.keyType = keyType;
    this.valueType = valueType;

    MaterializedField innerVectorField = MaterializedField.create(FIELD_INNER_REPEATED_STRUCT_NAME, RepeatedMapVector.TYPE); // todo: last arg could be RepeatedMapVector.TYPE
    innerVector = new RepeatedMapVector(innerVectorField, allocator, callBack);
    addChildToInner(FIELD_KEY_NAME, keyType, allocator);
    addChildToInner(FIELD_VALUE_NAME, valueType, allocator);

    this.field.addChild(innerVectorField);

    reader = new SingleTrueMapReaderImpl(TrueMapVector.this);
  }

  private void addChildToInner(String name, MajorType type, BufferAllocator allocator) {
    ValueVector vector = BasicTypeHelper.getNewVector(MaterializedField.create(name, type), allocator);
    putChildIntoInner(name, vector);
  }

  @Override
  public SingleTrueMapReaderImpl getReader() {
    return reader;
  }

  public void copyFromSafe(int inIndex, int outIndex, TrueMapVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public void copyFrom(int inIndex, int outIndex, TrueMapVector from) { // todo: get inspired
    final FieldReader in = from.getReader();
    in.setPosition(inIndex);
    final FieldWriter out = getWriter();
    out.setPosition(outIndex);
    ComplexCopier.copy(in, out);
  }

  // todo: looks like this one is used when query contains LIMIT
  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    copyFromSafe(fromIndex, toIndex, (TrueMapVector) from);
  }

  @Deprecated
  @Override // todo: remove
  protected boolean supportsDirectRead() { // todo: what the heck?
    return true;
  }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    innerVector.setInitialCapacity(numRecords);
  }

  @Override
  public int getBufferSize() {
    checkInitialized();
    return innerVector.getBufferSize();
  }

  @Override
  public int getAllocatedSize() {
    return innerVector.getAllocatedSize();
  }

  public boolean initialized() {
    return size() == NUMBER_OF_CHILDREN;
  }

  private void checkInitialized() {
    assert initialized() : "TrueMapVector is not initialized";
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    checkInitialized();
    return innerVector.getBufferSizeFor(valueCount);
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    checkInitialized();
    return innerVector.getBuffers(clear);
  }

  @Override
  protected Collection<String> getChildFieldNames() {
    return fieldNames;
  }

  public void transferTo(TrueMapVector target) {
    innerVector.makeTransferPair(target.innerVector);
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
// todo: getField()?
    public MapTransferPair(TrueMapVector from, String path, BufferAllocator allocator) {
      this(from, new TrueMapVector(MaterializedField.create(path, from.getField().getType()), allocator, new SchemaChangeCallBack(), from.getKeyType(), from.getValueType()), false);
    }

    public MapTransferPair(TrueMapVector from, TrueMapVector to) {
      this(from, to, true);
    }

    protected MapTransferPair(TrueMapVector from, TrueMapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.to.keyType = this.from.keyType;
      this.to.valueType = this.from.valueType;
      // todo: not sure if to remove
      // this.to.valueCount = this.from.valueCount;
      this.pairs = new TransferPair[from.size()];

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
    public void transfer() { // todo: this one of interest
      for (final TransferPair p : pairs) {
        p.transfer();
      }
      to.valueCount = from.valueCount;
      // todo: remove?
      // to.offsets.getMutator().setValueCount(from.offsets.getAccessor().getValueCount());
      from.clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      for (TransferPair p : pairs) {
        p.copyValueSafe(from, to); // todo: is the problem here/
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
    checkInitialized();
    return innerVector.getValueCapacity();
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
      // ValueVector vector = addOrGet(fieldDef.getName(), )
      if (vector == null) { // todo: this should not be the case, should it?
        // if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, allocator);
        // vector = innerVector.addOrGet(fieldDef.getName(), fieldDef.getType(), ValueVector.class); // todo: or this one?
        putChildIntoInner(fieldDef.getName(), vector); // todo: what to do?
      }
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
  @Deprecated // todo: not sure if this is OK, or to use innerVector.addOrGet(...)
  public void putChildIntoInner(String name, ValueVector vector) { // todo: actually this might be useful to add innerVector to the TrueMapVector itself
//    putVector(name, vector);
//    field.addChild(vector.getField());
    innerVector.putChild(name, vector);
  }

//  @Deprecated
//  protected void putVector(String name, ValueVector vector) {
//    Preconditions.checkNotNull(name, "field name cannot be null");
//    /*ValueVector old = vectors.put(
//        Preconditions.checkNotNull(name, "field name cannot be null").toLowerCase(),
//        Preconditions.checkNotNull(vector, "vector cannot be null")
//    );*/
//    // ValueVector old = null;
//    MajorType type = vector.getField().getType();
//    if (FIELD_KEY_NAME.equals(name)) {
//      // old = keys;
//      keys = vector;
//      keyType = type;
//    } else if (FIELD_VALUE_NAME.equals(name)) {
//      // old = values;
//      values = vector;
//      valueType = type;
//    } else if (FIELD_OFFSET_NAME.equals(name)) { // todo: this should be removed
//      offsets = (UInt4Vector) vector; // todo: revise
//    } else if (FIELD_LENGTH_NAME.equals(name)) { // todo: this should be removed
//      lengths = (UInt4Vector) vector; // todo: revise
//    } else {
//      logger.warn("Unknown field name '{}' put into {}", name, getClass().getName());
//    }
//    /*if (old != null && old != vector) {
//      logger.debug("Field [{}] mutated from [{}] to [{}]", name, old.getClass().getSimpleName(),
//          vector.getClass().getSimpleName());
//    }*/
//  }

  @Override
  public ValueVector getChild(String name) {
    if (!fieldNames.contains(name)) { // todo: change to assert?
      throw new DrillRuntimeException("TrueMapVector has 'key' and 'value' ValueVectors only");
    }
    return innerVector.getChild(name);
  }

  @Override
  public SerializedField getMetadata() {
    checkInitialized();
    SerializedField.Builder b = getField()
        .getAsBuilder()
        .setBufferLength(getBufferSize())
        .setValueCount(valueCount);

//    for (ValueVector v : children) {
//      b.addChild(v.getMetadata());
//    }
    b.addChild(innerVector.getMetadata());
    return b.build();
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public class Accessor extends BaseValueVector.BaseAccessor {

    @Override
    public Object getObject(int index) {
      int offset = 0;
      int length = 0;
      UInt4Vector offsets = getInnerOffsets();
      if (index > 0) { // todo: this may contain errors
        offset = offsets.getAccessor().get(index - 1);
        length = offsets.getAccessor().get(index) - offset;
      }
      ValueVector keys = getChild(FIELD_KEY_NAME);
      ValueVector values = getChild(FIELD_VALUE_NAME);

      Map<Object, Object> result = new JsonStringHashMap<>();
      for (int i = 0; i < length; i++) {
        int valIndex = offset + i;
        Object keyVal = keys.getAccessor().getObject(valIndex);
        result.put(keyVal, values.getAccessor().getObject(valIndex));
      }
//      Map<Object, Object> result = new JsonStringHashMap<>(); // todo: change (probably to LinkedHashMap)
//      int offset = 0;
//      if (index > 0) {
//        offset = offsets.getAccessor().get(index - 1);
//      }
//      int length = lengths.getAccessor().get(index);
//      for (int i = 0; i < length; i++) {
//        int valIndex = offset + i;
//        Object keyVal = keys.getAccessor().getObject(valIndex);
//        result.put(keyVal, values.getAccessor().getObject(valIndex));
//      }
      return result;
    }

    @Override
    public boolean isNull(int index) {
      return false; // todo: redefine if Nullable
    }

    // todo: what to do?
    /*public void get(int index, ComplexHolder holder) {
      reader.setPosition(index); // todo: this is likely not right!
      holder.reader = reader;
    }*/

    public void get(int index, TrueMapHolder holder) {
      // reader.setPosition(index); // todo: this is likely not right!
      holder.vector = TrueMapVector.this;
      holder.reader = reader;
      holder.start = index > 0 ? getInnerOffsets().getAccessor().get(index - 1) : 0;
      holder.end =  getInnerOffsets().getAccessor().get(index);
    }

    // todo: introduce getKey(int index, ValueHolder holder)?
    // todo: introduce getValue(int index, ValueHolder holder)?

    @Override
    public int getValueCount() {
      return valueCount;
    }
  }

  // todo: move this logic to TrueMapWriter?
  public class Mutator extends BaseValueVector.BaseMutator {

    @Override
    public void setValueCount(int valueCount) {
      checkInitialized();
      innerVector.getMutator().setValueCount(valueCount);
    }

    /*public void setNull() {
    }*/

    @Override // todo: override?
    public void reset() {
    }

    @Override
    public void generateTestData(int values) {
    }
  }

  @Override
  public void clear() {
//    for (ValueVector v : children) {
//      v.clear();
//    }
    innerVector.clear();
    valueCount = 0;
  }

  @Override
  public void close() {
     // super.close();
    innerVector.close();
    valueCount = 0;
  }

  @Override
  public void toNullable(ValueVector nullableVector) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void exchange(ValueVector other) {
    TrueMapVector otherMap = (TrueMapVector) other;
    if (innerVector.size() != otherMap.innerVector.size()) {
      throw new IllegalStateException("Maps have different column counts");
    }
    innerVector.exchange(otherMap.innerVector);
    int temp = otherMap.valueCount;
    otherMap.valueCount = valueCount;
    valueCount = temp;
  }

  @Override // todo: prettify
  public VectorWithOrdinal getChildVectorWithOrdinal(String name) { // todo: discard?
    assert fieldNames.contains(name) : "Message goes here";
    ValueVector vector = getChild(name);
    if (name.equals(FIELD_KEY_NAME)) { // todo: create
      return new VectorWithOrdinal(vector, 0);
    } else if (name.equals(FIELD_VALUE_NAME)) {
      return new VectorWithOrdinal(vector, 1);
    } else {
      logger.warn("Field with name '{}' was not found.");
      return null;
    }
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return innerVector.iterator();
  }

  @Override
  MajorType getLastPathType() { // todo: probably introduce another method
    // return super.getLastPathType(); // todo: return key type? // todo: even value type?
    return getChild(FIELD_VALUE_NAME).getField().getType();
  }

  @Override
  public int size() {
    return innerVector.size();
  }

  // todo: from AbstractMapVector
  // todo: re-implement!
  public <T extends ValueVector> T addOrGet(String name, TypeProtos.MajorType type, Class<T> clazz) { // todo: this should be revised
    // todo:
//    if (!name.equals(FIELD_KEY_NAME) && !name.equals(FIELD_VALUE_NAME) && !name.equals(FIELD_OFFSET_NAME) && !name.equals(FIELD_LENGTH_NAME)) {
//      logger.warn("No field {} in True map!", name);
//      return null;
//    }
    // if (!name.equals(FIELD_KEY_NAME) && !name.equals(FIELD_VALUE_NAME)) {
    if (!fieldNames.contains(name)) {
      logger.warn("No field {} in True map!", name);
      throw new DrillRuntimeException(String.format("Not allowed field '%s' in TrueMapVector", name));
    }
    ValueVector existing = getChild(name);
    boolean create = false;
    if (existing == null) {
      create = true;
    } else if (clazz.isAssignableFrom(existing.getClass())) {
      return (T) existing;
    } else if (nullFilled(existing)) { // todo: not sure whether need this part, Probably, remove
      existing.clear();
      // Since it's removing old vector and adding new one based on new type, it should do same for Materialized field,
      // Otherwise there will be duplicate of same field with same name but different type.
      field.removeChild(existing.getField());
      create = true;
    }
    if (create) {
      final T vector = (T) BasicTypeHelper.getNewVector(name, allocator, type, callBack);
      putChildIntoInner(name, vector);
      if (callBack != null) {
        callBack.doWork();
      }
      if (allChildrenPresent()) {
        allocateChildren();
//        reader.init();
      }
      return vector;
    }
    final String message = "Drill does not support schema change yet. Existing[%s] and desired[%s] vector types mismatch";
    throw new IllegalStateException(String.format(message, existing.getClass().getSimpleName(), clazz.getSimpleName()));
  }
  // todo: reimplement this method to acoount for innerVector
  public TrueMapVector addOrGet(String name, MajorType type, MajorType keyType, MajorType valueType) {
    // todo:
    /*if (!name.equals(FIELD_KEY_NAME) && !name.equals(FIELD_VALUE_NAME) && !name.equals(FIELD_OFFSET_NAME) && !name.equals(FIELD_LENGTH_NAME)) {
      logger.warn("No field {} in True map!", name);
      return null;
    }*/
    ValueVector existing = getChild(name);
    boolean create = false;
    if (existing == null) {
      create = true;
    } /*else if (clazz.isAssignableFrom(existing.getClass())) {
      return (T) existing;
    }*/ else if (nullFilled(existing)) { // todo: not sure whether need this part, Probably, remove // todo: is this needed?
      existing.clear();
      // Since it's removing old vector and adding new one based on new type, it should do same for Materialized field,
      // Otherwise there will be duplicate of same field with same name but different type.
      field.removeChild(existing.getField());
      create = true;
    }
    if (create) {
//      TrueMapVector vector = BasicTypeHelper.getNewMapVector(name, allocator, callBack, keyType, valueType);
      TrueMapVector vector = (TrueMapVector) BasicTypeHelper.getNewVector(name, allocator, type, callBack);
      putChildIntoInner(name, vector);
      if (callBack != null) {
        callBack.doWork();
      }
//      if (allChildrenPresent()) {
//        allocateChildren();
//        reader.init();
//      }
      return vector;
    }
    final String message = "Drill does not support schema change yet. Existing[%s] and desired[%s] vector types mismatch";
    throw new IllegalStateException(String.format(message, existing.getClass().getSimpleName(), TrueMapVector.class.getName())); // todo: remove
  }

  public TrueMapVector addOrGet(String name, MajorType type, MajorType keyType, List<MajorType> valueType) {
    // todo:
    /*if (!name.equals(FIELD_KEY_NAME) && !name.equals(FIELD_VALUE_NAME) && !name.equals(FIELD_OFFSET_NAME) && !name.equals(FIELD_LENGTH_NAME)) {
      logger.warn("No field {} in True map!", name);
      return null;
    }*/
    ValueVector existing = getChild(name);
    boolean create = false;
    if (existing == null) {
      create = true;
    } /*else if (clazz.isAssignableFrom(existing.getClass())) {
      return (T) existing;
    }*/ else if (nullFilled(existing)) { // todo: not sure whether need this part, Probably, remove
      existing.clear();
      // Since it's removing old vector and adding new one based on new type, it should do same for Materialized field,
      // Otherwise there will be duplicate of same field with same name but different type.
      field.removeChild(existing.getField());
      create = true;
    }
    if (create) {
//      TrueMapVector vector = BasicTypeHelper.getNewMapVector(name, allocator, callBack, keyType, valueType.get(0));
      TrueMapVector vector = (TrueMapVector) BasicTypeHelper.getNewVector(name, allocator, type, callBack);
      putChildIntoInner(name, vector);
      if (callBack != null) {
        callBack.doWork();
      }
//      if (allChildrenPresent()) {
//        allocateChildren();
//        reader.init();
//      }
      return vector;
    }
    final String message = "Drill does not support schema change yet. Existing[%s] and desired[%s] vector types mismatch";
    throw new IllegalStateException(String.format(message, existing.getClass().getSimpleName(), TrueMapVector.class.getName())); // todo: remove
  }

  private boolean allChildrenPresent() {
    // todo: lengths and offsets are likely to be present all the time. Discard?
    return getChild(FIELD_KEY_NAME) != null && getChild(FIELD_VALUE_NAME) != null;
  }

  // todo: remove!
  @Deprecated
  private void allocateChildren() {
//    children = new ArrayList<>(Arrays.asList(getChild(FIELD_KEY_NAME), getChild(FIELD_VALUE_NAME))); // todo: do smth better than ArrayList<>
//    for (ValueVector child : children) { // todo: ListVector does not add children to field, for example. Maybe discard it here also?
//      // todo: this may be not safe as the same map field can be reused (recursively or...). For ex., this can modify value field if value is a TrueMap
//      field.addChild(child.getField());
//    }
//    writer = new TrueMapWriter(this, null, keyType, valueType); // todo: this is likely to be shit
  }
  // todo: is this method needed and what is the  purpose?
  private boolean nullFilled(ValueVector vector) { // todo: revisit this method as its purpose is unclear
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
    assert fieldNames.contains(name) : "No such field in TrueMapVector";
    return innerVector.getChild(name, clazz);
  }

  // from AbstractMapVector
  @Override
  public boolean allocateNewSafe() { // todo: review
    return innerVector.allocateNewSafe();
    /* boolean to keep track if all the memory allocation were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
//    boolean success = false;
//    try {
//      for (ValueVector v : children) {
//        if (!v.allocateNewSafe()) {
//          return false;
//        }
//      }
//      success = true;
//    } finally {
//      if (! success) {
//        clear();
//      }
//    }
//    return true;
  }

  @Override
  public void collectLedgers(Set<AllocationManager.BufferLedger> ledgers) { // todo: what the heck?
  }

  @Override
  public int getPayloadByteCount(int valueCount) {
    return innerVector.getPayloadByteCount(valueCount);
  }

  // todo: or separate?
  public static class SingleTrueMapReaderImpl extends AbstractFieldReader {

    private final TrueMapVector vector;
    @Deprecated
    private int currentOffset;
    @Deprecated
    private int maxOffset;

    public SingleTrueMapReaderImpl(TrueMapVector vector) {
      this.vector = vector;
//      init();
    }

//    void init() {
//      keyReader = vector.getKeys().getReader();
//      valueReader = vector.getValues().getReader();
//      children = Collections.unmodifiableList(Arrays.asList(keyReader, valueReader, vector.getInnerOffsets().getReader()));
//    }

//    private void setChildrenPosition(int index){
//      // for(FieldReader r : fields.values()){
//      for(FieldReader r : children){
//        r.setPosition(index);
//      }
//    }

    @Override // todo: check where this method is invoked and do changes
    public FieldReader reader(String name){
      // todo: NOTICE: before there was position set to child Readers!
      assert fieldNames.contains(name);
      return vector.getInnerReader().reader(name);
    }

    @Override
    public void setPosition(int index) { // todo: make sure this works
      super.setPosition(index);
      vector.getInnerReader().setPosition(index);
//      currentOffset = (index > 0 ? vector.offsets.getAccessor().get(index - 1) : 0) - 1;
//      maxOffset = index > 0 ? vector.offsets.getAccessor().get(index) : 0;
    }

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
        idx = vector.getInnerOffsets().getAccessor().get(idx() + 1);
//        int offset = idx() > 0 ? vector.getInnerOffsets().getAccessor().get(idx() - 1) : 0;
        int offset = vector.getInnerOffsets().getAccessor().get(idx());
      //}
      int index = NOT_FOUND;
      // for (int i = 0; i < vector.lengths.getAccessor().get(idx()); i++) {
      // todo: if key-uniqueness is not guaranteed, then return the last one for the key
      for (int i = 0; i < idx - offset; i++) {
        if (vector.getChild(FIELD_KEY_NAME).getAccessor().getObject(offset + i).toString().equals(key)) {
        // if (vector.keys.getAccessor().getObject(i).equals(((Integer) key).longValue())) {
          index = offset + i;
          break;
        }
      }

      boolean found = index != NOT_FOUND;
      /*NullableIntHolder h = (NullableIntHolder) holder;
      h.isSet = found ? 1 : 0;
      if (found) {
        h.value = (int) vector.values.getAccessor().getObject(index);
      }*/

      // return index != -1;
      return index;
      /*int offset = 0;
      if (index > 0) {
        offset = offsets.getAccessor().get(index - 1);
      }
      int length = lengths.getAccessor().get(index);
      for (int i = 0; i < length; i++) {
        int valIndex = offset + i;
        Object keyVal = keys.getAccessor().getObject(valIndex);
        result.put(keyVal, values.getAccessor().getObject(valIndex));
      }*/
    }

    public void read(Object key, ValueHolder holder) {
      int index = find(key);
      // int prevIndex = valueReader.idx();
      // todo: decide if uncomment!
      FieldReader valueReader = vector.getInnerReader().reader(FIELD_VALUE_NAME);
      valueReader.setPosition(index);
      // todo: pass the index into the method instead of setting position to the reader
      if (index > NOT_FOUND) { // todo: actually check for inequality
        valueReader.read(index, holder); // todo: uncomment and add to FieldReader interface. Implement for every reader (cast to that's reader ValueHolder, perhaps)
      }
      // valueReader.setPosition(prevIndex);
    }

    public void read(Object key, ComplexHolder holder) {
      int index = find(key);
      if (index == NOT_FOUND) {
        holder.isSet = 0;
        // todo: include holder.reader = valueReader?
        return;
      } else {
        holder.isSet = 1;
      }
      FieldReader valueReader = vector.getInnerReader().reader(FIELD_VALUE_NAME);
      valueReader.setPosition(index);
      // holder.isSet = valueReader.isSet() ? 1 : 0;
      holder.reader = valueReader;
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
    public void copyAsValue(BaseWriter.MapWriter writer){ // todo: implement this and copyAsValue(...)
      TrueMapWriter impl = (TrueMapWriter) writer;
      throw new AssertionError("TrueMapVector#copyAsValue(BaseWriter.MapWriter) is not implemented");
      // impl.container.copyFromSafe(idx(), impl.idx(), vector);
    }

    @Override
    public void copyAsField(String name, BaseWriter.MapWriter writer){
      SingleMapWriter impl = (SingleMapWriter) writer.map(name);
      throw new AssertionError("TrueMapVector#copyAsField(String, BaseWriter.MapWriter) is not implemented");
      // impl.container.copyFromSafe(idx(), impl.idx(), vector);
    }

    @Override // todo: see UnionListReader
    public boolean next() {
      if (currentOffset + 1 < maxOffset) { // todo: this can be wrong!
        vector.getReader().setPosition(++currentOffset);
        return true;
      } else {
        return false;
      }
      // return vector.innerVector.next(); // todO: why no such method?
    }
    // todo: use this?
    public void copyAsValue(TrueMapWriter writer) {
      ComplexCopier.copy(this, writer);
    }
  }

  public boolean isEmpty(int index) {
    // todo: check if offsets is not empty
    // return lengths.getAccessor().get(index) == 0;
    // todo: this is BS
    return getInnerOffsets().getAccessor().get(index) == 0; // todo: check if allocated
  }

  // todo: this is not right, probably (consider case of repeated truemap)
  public UInt4Vector getInnerOffsets() { // todo: make this private and use getOffset(index) and setOffset(index,offset) instead
    return innerVector.getOffsetVector();
  }

  // todo: add javadoc
  public int getInnerOffset(int index) {
    return getInnerOffsets().getAccessor().get(index);
  }

  // todo: add javadoc
  public void setInnerOffset(int index, int offset) {
    getInnerOffsets().getMutator().setSafe(index, offset);
  }

  private FieldReader getInnerReader() {
    return innerVector.getReader();
  }

  public ValueVector getKeys() {
    return getChild(FIELD_KEY_NAME);
  }

  public ValueVector getValues() {
    return getChild(FIELD_VALUE_NAME);
  }

  @Deprecated // todo: there is no need of the method, is there?
  public void setWriter(TrueMapWriter writer) {
//    if (this.writer != null) {
//      throw new DrillRuntimeException("Writer is already set to TrueMapVector");
//    }
    this.writer = writer;
  }

  @Deprecated // todo: remove
  public TrueMapWriter getWriter() {
    return writer;
  }

  public MajorType getKeyType() {
    return keyType;
  }

  public MajorType getValueType() {
    return valueType;
  }

  public RepeatedMapVector getDataVector() {
    return innerVector;
  }
}
