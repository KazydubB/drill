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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

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
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;
import org.apache.drill.exec.vector.complex.impl.TrueMapWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Ordering;
import org.apache.drill.shaded.guava.com.google.common.primitives.Ints;
// todo: consider implementing RepeatedValueVector...
public class TrueMapVector extends AbstractContainerVector {

  public final static MajorType TYPE = Types.required(MinorType.TRUEMAP); // todo: change!
  private final static List<MinorType> supportedKeyTypes = Collections.unmodifiableList(Arrays.asList(MinorType.INT, MinorType.VARCHAR, MinorType.BIGINT));
  private final static List<MinorType> complexTypes = Collections.unmodifiableList(Arrays.asList(MinorType.MAP, MinorType.TRUEMAP, MinorType.LIST, MinorType.UNION));

  // todo: remove:
  private static int created;
  private static int removed;

  public static final String FIELD_KEY_NAME = "key";
  public static final String FIELD_VALUE_NAME = "value";
  // todo: do not use name for the field and use getOffsets()
  public static final String FIELD_OFFSET_NAME = "$offset$";
  @Deprecated
  public static final String FIELD_LENGTH_NAME = "$length$"; // todo: remove if unneeded
  public static final List<String> fieldNames = Collections.unmodifiableList(Arrays.asList(FIELD_KEY_NAME, FIELD_VALUE_NAME, FIELD_OFFSET_NAME, FIELD_LENGTH_NAME));
  public static final int NUMBER_OF_CHILDREN = 4;

  // todo: make sure it's OK to have static field for the purpose
  private static final MaterializedField OFFSETS_FIELD = MaterializedField.create(FIELD_OFFSET_NAME, Types.required(TypeProtos.MinorType.UINT4));
  private static final MaterializedField LENGTHS_FIELD = MaterializedField.create(FIELD_LENGTH_NAME, Types.required(TypeProtos.MinorType.UINT4));

  // todo: add parent writer? probably not
  // todo: consider fixed 'name's for vectors (key and value) // todo: make final
  private ValueVector keys;
  private ValueVector values; // private final List<ValueVector> children; // todo: uncomment
  @Deprecated
  private UInt4Vector lengths; // todo: make vectors final fields? // todo: rename to offsets!
  // NOTE: offsets doesn't store the first 0 offset.
  private UInt4Vector offsets; // todo: remove lengths vector and use offsets only!
  // todo: consider bits vv for nullability...
  @Deprecated
  private TrueMapWriter writer; // todo: remove!?

  private /*final*/ List<ValueVector> children = Collections.emptyList(); // todo: this may be tricky
  // todo: remove the types?
  private /*final*/ MajorType keyType;
  private /*final*/ MajorType valueType;

  private /*final*/ boolean primitiveValueType;

  private final SingleTrueMapReaderImpl reader;//  = new SingleTrueMapReaderImpl(TrueMapVector.this);

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  private int valueCount;

  private boolean initialized; // todo: remove

  // todo: remove DEPRECATION?
  // @Deprecated // todo: remove the method
  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    super(field, allocator, callBack);
    // boolean keyTypeSupported =
        // keyType.getMode() != TypeProtos.DataMode.REPEATED && supportedKeyTypes.contains(keyType.getMinorType());
        // keyType.getMode() == TypeProtos.DataMode.REQUIRED && supportedKeyTypes.contains(keyType.getMinorType());
    // Preconditions.checkArgument(keyTypeSupported, "Unsupported key type in TRUEMAP: " + keyType + ". Key should be REQUIRED primitive type");
//    this.keyType = keyType;
//    this.valueType = valueType;
//    keys = BasicTypeHelper.getNewVector(FIELD_KEY_NAME, allocator, keyType, callBack);
//    values = BasicTypeHelper.getNewVector(FIELD_VALUE_NAME, allocator, valueType, callBack);
    offsets = new UInt4Vector(OFFSETS_FIELD, allocator);
    lengths = new UInt4Vector(LENGTHS_FIELD, allocator);
    // todo: might want to remove:
    // children = Collections.unmodifiableList(Arrays.asList(keys, values, lengths, offsets));
    // children = Arrays.asList(lengths, offsets); // todo: order matters!
//    for (ValueVector child : children) { // todo: ListVector does not add children to field, for example. Maybe discard it here also?
//      // todo: this may be not safe as the same map field can be reused (recursively or...). For ex., this can modify value field if value is a TrueMap
//      field.addChild(child.getField());
//    }
    MinorType k = keyType == null ? null : keyType.getMinorType();
    MinorType v = valueType == null ? null : valueType.getMinorType();
//    System.out.println(String.format("TrueMapVector(%s, %s) created: %d", k, v, ++created));
    reader = new SingleTrueMapReaderImpl(TrueMapVector.this, true);
  }

  public TrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack, MajorType keyType, MajorType valueType) {
    super(field, allocator, callBack);
    boolean keyTypeSupported =
        // keyType.getMode() != TypeProtos.DataMode.REPEATED && supportedKeyTypes.contains(keyType.getMinorType());
        keyType.getMode() == TypeProtos.DataMode.REQUIRED && supportedKeyTypes.contains(keyType.getMinorType());
    Preconditions.checkArgument(keyTypeSupported, "Unsupported key type in TRUEMAP: " + keyType + ". Key should be REQUIRED primitive type");
    this.keyType = keyType;
    this.valueType = valueType;
    keys = BasicTypeHelper.getNewVector(FIELD_KEY_NAME, allocator, keyType, callBack);
    values = BasicTypeHelper.getNewVector(FIELD_VALUE_NAME, allocator, valueType, callBack); // todo: handle values of Complex types separately?
    offsets = new UInt4Vector(OFFSETS_FIELD, allocator);
    lengths = new UInt4Vector(LENGTHS_FIELD, allocator);
//    children = Collections.unmodifiableList(Arrays.asList(keys, values, lengths, offsets));
//    for (ValueVector child : children) { // todo: ListVector does not add children to field, for example. Maybe discard it here also?
//      // todo: this may be not safe as the same map field can be reused (recursively or...). For ex., this can modify value field if value is a TrueMap
//      field.addChild(child.getField());
//    }
    allocateChildren();

    reader = new SingleTrueMapReaderImpl(TrueMapVector.this);

    primitiveValueType = valueType.getMode() != TypeProtos.DataMode.REPEATED && !complexTypes.contains(valueType.getMinorType());

    initialized = true;
    MinorType k = keyType == null ? null : keyType.getMinorType();
    MinorType v = valueType == null ? null : valueType.getMinorType();
//    System.out.println(String.format("TrueMapVector(%s, %s) created: %d", k, v, ++created));
  }

  @Override
  public SingleTrueMapReaderImpl getReader() {
    return reader;
  }

  // transient private MapTransferPair ephPair;
  // transient private MapSingleCopier ephPair2;

//  public void copyFromSafe(int fromIndex, int thisIndex, TrueMapVector from) {
//    if (ephPair == null || ephPair.from != from) {
//      ephPair = (MapTransferPair) from.makeTransferPair(this);
//    }
//    ephPair.copyValueSafe(fromIndex, thisIndex);
//  }

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

  // todo: Make RepeatedTrueMapVector
//  public void copyFromSafe(int fromSubIndex, int thisIndex, RepeatedMapVector from) { // todo: not sure what is the purpose
//    if (ephPair2 == null || ephPair2.from != from) {
//      // ephPair2 = from.makeSingularCopier(this);
//    }
//    // ephPair2.copySafe(fromSubIndex, thisIndex);
//  }
  // todo: looks like this one is used when query contains LIMIT
  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    // System.out.println("No idea!");// copyFromSafe(fromIndex, toIndex, (MapVector) from); // todo: implement for where clause?
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
  protected Collection<String> getChildFieldNames() {
    return fieldNames;
  }

  public void transferTo(TrueMapVector target) {
    throw new AssertionError("TrueMapVector#transferTo(TrueMapVector) is not implemented");
//    offsets.makeTransferPair(target.offsets).transfer();
//    bits.makeTransferPair(target.bits).transfer();
//    if (target.getDataVector() instanceof ZeroVector) {
//      target.addOrGetVector(new VectorDescriptor(vector.getField().getType()));
//    }
//    getDataVector().makeTransferPair(target.getDataVector()).transfer();
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
      this(from, new TrueMapVector(MaterializedField.create(path, TYPE), allocator, new SchemaChangeCallBack(), from.getKeyType(), from.getValueType()), false);
      // this(from, new TrueMapVector(), false);
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
//      this.to.ephPair = null;
//      this.to.ephPair2 = null;

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
//          System.out.println("Allocated child: " + child);
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
    if (size() == 0) { // todo: this can be removed as this always has 4 children
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
      // ValueVector vector = addOrGet(fieldDef.getName(), )
      if (vector == null) { // todo: this should not be the case, should it?
        // if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, allocator);
        putChild(fieldDef.getName(), vector); // todo: what to do?
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
  @Deprecated // todo: not sure if to deprecate yet
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
    MajorType type = vector.getField().getType();
    if (FIELD_KEY_NAME.equals(name)) {
      // old = keys;
      keys = vector;
      keyType = type;
    } else if (FIELD_VALUE_NAME.equals(name)) {
      // old = values;
      values = vector;
      valueType = type;
    } else if (FIELD_OFFSET_NAME.equals(name)) { // todo: this should be removed
      offsets = (UInt4Vector) vector; // todo: revise
    } else if (FIELD_LENGTH_NAME.equals(name)) { // todo: this should be removed
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
//      if (index > 0) {
//        offset = offsets.getAccessor().get(index - 1);
//        length = offsets.getAccessor().get(index) - offset;
//      }
//      for (int i = 0; i < length; i++) {
//        int valIndex = offset + i;
//        Object keyVal = keys.getAccessor().getObject(valIndex);
//        result.put(keyVal, values.getAccessor().getObject(valIndex));
//      }
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
      holder.start = index > 0 ? offsets.getAccessor().get(index - 1) : 0;
      holder.end =  offsets.getAccessor().get(index);
    }

    // todo: introduce getKey(int index, ValueHolder holder)?
    // todo: introduce getValue(int index, ValueHolder holder)?

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
    /*private int currentRow = -1;
    private int length = -1; // (designates length for current row)
    private boolean rowStarted;

    @Deprecated
    public void startRow() {
      currentRow++;
      length = 0;
      rowStarted = true;
    }

    @Deprecated
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

    @Deprecated
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
      if (vector instanceof NullableIntVector) { // todo: not instanceof but type?
        ((NullableIntVector) vector).getMutator().setSafe(index, (int) value);
      } else if (vector instanceof NullableVarCharVector) {
        byte[] keyData = (byte[]) value;
        ((NullableVarCharVector) vector).getMutator().setSafe(index, keyData, 0, keyData.length);
      }
    }*/

    @Override
    public void setValueCount(int valueCount) { // todo: change keys and values to reflect offset as count?
      int childValueCount = valueCount > 0 ? offsets.getAccessor().get(valueCount - 1) : 0;
      keys.getMutator().setValueCount(childValueCount); // todo: revisit
//      if (keys.getField().getType().getMinorType() == MinorType.TRUEMAP) {
//        ((TrueMapVector) values).getMutator().setValueCount(c);
//      } else {
//        values.getMutator().setValueCount(childValueCount); // todo: revisit this one!
//      }
//      if (values.getField().getType().getMinorType() != MinorType.TRUEMAP) {
        values.getMutator().setValueCount(childValueCount);
//      }
      offsets.getMutator().setValueCount(valueCount);
      lengths.getMutator().setValueCount(valueCount);
      setMapValueCount(valueCount);
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
  public void clear() { // todo: uncomment
//    if (!initialized) {
//      return;
//    }
    for (ValueVector v : children) {
      v.clear();
    }
//    if (data != null) {
//      data.release();
//    }
//    data = allocator.getEmpty();
//    super.clear();

    // todo: probably remove?
    // children.clear();
    valueCount = 0;
    // todo: remove
//    for (ValueVector v : this) {
//      v.clear();
//    }

    MinorType k = keyType != null ? keyType.getMinorType() : null;
    MinorType v = valueType != null ? valueType.getMinorType() : null;
//    System.out.println("Clearing TrueMapVector: (" + k + ", " + v + ")");
    // System.out.println("TrueMapVector removed: " + ++removed);
  }

//  @Override
//  public void close() {
//    // final Collection<ValueVector> vectors = getChildren();
//    /*for (ValueVector v : children) {
//      v.close();
//    }*/
//    // vectors.clear(); this seems unnecessary, is it?
////    if (!initialized) {
////      return;
////    }
//    super.close();
//    valueCount = 0;
//  }

  @Override
  public void close() {
    /*for (ValueVector v : children) {
      v.close();
    }
    children.clear();*/
//    clear();
//    children.clear();
    // valueCount = 0;
//    System.out.println("TrueMapVector removed: " + ++removed);
    // super.close(); // todo: needed?

    // todo: from MapVector
    final Collection<ValueVector> vectors = children;
    for (final ValueVector v : vectors) {
      v.close();
    }
    vectors.clear();
    valueCount = 0;

    for (final ValueVector valueVector : vectors) {
      valueVector.close();
    }
    vectors.clear();
    for (ValueVector vector : this) {
      vector.close();
    }
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
    if (children.size() != otherMap.children.size()) { // todo: what this valueCount means?
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
      putChild(name, vector);
      if (callBack != null) {
        callBack.doWork();
      }
      if (allChildrenPresent()) {
        allocateChildren();
        reader.init();
      }
      return vector;
    }
    final String message = "Drill does not support schema change yet. Existing[%s] and desired[%s] vector types mismatch";
    throw new IllegalStateException(String.format(message, existing.getClass().getSimpleName(), clazz.getSimpleName()));
  }
  // todo: add the method to parent
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
      putChild(name, vector);
      if (callBack != null) {
        callBack.doWork();
      }
      if (allChildrenPresent()) {
        allocateChildren();
        reader.init();
      }
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
      putChild(name, vector);
      if (callBack != null) {
        callBack.doWork();
      }
      if (allChildrenPresent()) {
        allocateChildren();
        reader.init();
      }
      return vector;
    }
    final String message = "Drill does not support schema change yet. Existing[%s] and desired[%s] vector types mismatch";
    throw new IllegalStateException(String.format(message, existing.getClass().getSimpleName(), TrueMapVector.class.getName())); // todo: remove
  }

  private boolean allChildrenPresent() {
    // todo: lengths and offsets are likely to be present all the time. Discard?
    return !(keys == null || values == null || lengths == null || offsets == null);
  }

  private void allocateChildren() {
    // children = Collections.unmodifiableList(Arrays.asList(keys, values, lengths, offsets));
    children = new ArrayList<>(Arrays.asList(keys, values, lengths, offsets));
    for (ValueVector child : children) { // todo: ListVector does not add children to field, for example. Maybe discard it here also?
      // todo: this may be not safe as the same map field can be reused (recursively or...). For ex., this can modify value field if value is a TrueMap
      field.addChild(child.getField());
    }
    writer = new TrueMapWriter(this, null, keyType, valueType); // todo: this is likely to be shit
  }
  // todo: is this method needed and what is the  purpose?
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
  public boolean allocateNewSafe() { // todo: review
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
  public void collectLedgers(Set<AllocationManager.BufferLedger> ledgers) { // todo: what the heck?
  }

  // todo: from AbstractMapVector
  @Override
  public int getPayloadByteCount(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    int count = 0;
    for (ValueVector v : children) {
      count += v.getPayloadByteCount(valueCount); // todo: ensure this is OK for all the children (i.e. all children have the same valueCount)
    }
    return count;
  }

  // todo: or separate?
  public static class SingleTrueMapReaderImpl extends AbstractFieldReader {

    private final TrueMapVector vector;
    private FieldReader keyReader;
    private FieldReader valueReader;
    private /*final*/ List<FieldReader> children;
    // todo: add offsetsReader?
    // private final Map<String, FieldReader> fields = Maps.newHashMap();

    private int currentOffset;
    private int maxOffset;

    public SingleTrueMapReaderImpl(TrueMapVector vector, boolean partial) {
      this.vector = vector;
    }

    public SingleTrueMapReaderImpl(TrueMapVector vector) {
      this.vector = vector;
//      keyReader = vector.getKeys().getReader();
//      valueReader = vector.getValues().getReader();
//      children = Collections.unmodifiableList(Arrays.asList(keyReader, valueReader));
      init();
    }

    void init() {
      keyReader = vector.getKeys().getReader();
      valueReader = vector.getValues().getReader();
      children = Collections.unmodifiableList(Arrays.asList(keyReader, valueReader, vector.getOffsets().getReader(), vector.getLengths().getReader()));
    }

    private void setChildrenPosition(int index){
      // for(FieldReader r : fields.values()){
      for(FieldReader r : children){
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
        logger.warn("True map does not contain field {}", name);
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

//    @Override // todo: this was before
//    public void setPosition(int index){
//      super.setPosition(index);
//      for(FieldReader r : children) { // todo: add offsetsReader
//        r.setPosition(index);
//      }
//    }

    @Override
    public void setPosition(int index) {
      super.setPosition(index);
      currentOffset = (index > 0 ? vector.offsets.getAccessor().get(index - 1) : 0) - 1;
      maxOffset = index > 0 ? vector.offsets.getAccessor().get(index) : 0;
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
        idx = vector.offsets.getAccessor().get(idx());
        int offset = idx() > 0 ? vector.offsets.getAccessor().get(idx() - 1) : 0;
      //}
      int index = -1;
      // for (int i = 0; i < vector.lengths.getAccessor().get(idx()); i++) {
      // todo: if key-uniqueness is not guaranteed, then return the last one for the key
      for (int i = 0; i < idx - offset; i++) {
        if (vector.keys.getAccessor().getObject(offset + i).toString().equals(key)) {
        // if (vector.keys.getAccessor().getObject(i).equals(((Integer) key).longValue())) {
          index = offset + i;
          break;
        }
      }

      boolean found = index != -1;
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
      valueReader.setPosition(index);
      // todo: pass the index into the method instead of setting position to the reader
      if (index > -1) {
        valueReader.read(index, holder); // todo: uncomment and add to FieldReader interface. Implement for every reader (cast to that's reader ValueHolder, perhaps)
      }
      // valueReader.setPosition(prevIndex);
    }

    public void read(Object key, ComplexHolder holder) {
      int index = find(key);
      valueReader.setPosition(index);
      holder.isSet = valueReader.isSet() ? 1 : 0;
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
      // impl.container.copyFromSafe(idx(), impl.idx(), vector);
    }

    @Override
    public void copyAsField(String name, BaseWriter.MapWriter writer){
      SingleMapWriter impl = (SingleMapWriter) writer.map(name);
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
    }
    // todo: use this?
    public void copyAsValue(TrueMapWriter writer) {
      ComplexCopier.copy(this, writer);
    }
  }

  public boolean isEmpty(int index) {
    // todo: check if offsets is not empty
    // return lengths.getAccessor().get(index) == 0;
    return offsets.getAccessor().get(index) == 0; // todo: check if allocated
  }

  public UInt4Vector getOffsets() {
    return offsets;
  }

  @Deprecated
  public UInt4Vector getLengths() {
    return lengths;
  }

  public ValueVector getKeys() {
    return keys;
  }

  public ValueVector getValues() {
    return values;
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
}
