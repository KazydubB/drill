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

import com.google.common.base.Preconditions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.holders.RepeatedTrueMapHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorDescriptor;
import org.apache.drill.exec.vector.complex.impl.RepeatedTrueMapReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import java.util.List;

public class RepeatedTrueMapVector extends BaseRepeatedValueVector {

  public final static TypeProtos.MajorType TYPE = Types.repeated(TypeProtos.MinorType.TRUEMAP);

  private final static String TRUEMAP_VECTOR_NAME = "$inner$";
  private  final static MaterializedField TRUEMAP_VECTOR_FIELD =
      MaterializedField.create(TRUEMAP_VECTOR_NAME, TrueMapVector.TYPE);

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  private final FieldReader reader = new RepeatedTrueMapReader(this);
  private final EmptyValuePopulator emptyPopulator;

  public RepeatedTrueMapVector(String path, BufferAllocator allocator) {
    this(MaterializedField.create(path, TYPE), allocator, null);
  }

  public RepeatedTrueMapVector(MaterializedField field, BufferAllocator allocator, CallBack callback) {
    super(field, allocator, new TrueMapVector(TRUEMAP_VECTOR_FIELD, allocator, callback));
    emptyPopulator = new EmptyValuePopulator(getOffsetVector());
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException();
    }
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return makeTransferPair(new RepeatedTrueMapVector(ref, allocator));
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new RepeatedTrueMapTransferPair((RepeatedTrueMapVector) target);
  }

  public class RepeatedTrueMapTransferPair implements TransferPair {
    private final RepeatedTrueMapVector target;
    private final TransferPair[] children;

    // todo: move-out to BaseRepeatedValueVector?
    public RepeatedTrueMapTransferPair(RepeatedTrueMapVector target) {
      this.target = Preconditions.checkNotNull(target);
      if (target.getDataVector() == DEFAULT_DATA_VECTOR) {
        target.addOrGetVector(VectorDescriptor.create(getDataVector().getField()));
        target.getDataVector().allocateNew();
      }
      this.children = new TransferPair[] {
          getOffsetVector().makeTransferPair(target.getOffsetVector()),
          getDataVector().makeTransferPair(target.getDataVector())
      };
    }

    @Override
    public void transfer() {
      for (TransferPair child : children) {
        child.transfer();
      }
    }

    @Override
    public ValueVector getTo() {
      return target;
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      target.allocateNew();
      for (int i = 0; i < length; i++) {
        copyValueSafe(startIndex + i, i);
      }
    }

    @Override // todo: move-out to BaseRepeatedValueVector?
    public void copyValueSafe(int srcIndex, int destIndex) {
      final RepeatedTrueMapHolder holder = new RepeatedTrueMapHolder();
      getAccessor().get(srcIndex, holder);
      target.emptyPopulator.populate(destIndex+1);
      final TransferPair vectorTransfer = children[1];
      int newIndex = target.getOffsetVector().getAccessor().get(destIndex);
      for (int i = holder.start; i < holder.end; i++, newIndex++) {
        vectorTransfer.copyValueSafe(i, newIndex);
      }
      target.getOffsetVector().getMutator().setSafe(destIndex + 1, newIndex);
    }
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    RepeatedTrueMapTransferPair pair = (RepeatedTrueMapTransferPair) from.makeTransferPair(this);
    pair.copyValueSafe(fromIndex, toIndex);
  }

  public class Accessor extends BaseRepeatedValueVector.BaseRepeatedAccessor {

    @Override
    public Object getObject(int index) {

      List<Object> list = new JsonStringArrayList<>();
      int start = offsets.getAccessor().get(index);
      int end = offsets.getAccessor().get(index + 1);
      for (int i = start; i < end; i++) {
        list.add(vector.getAccessor().getObject(i));
      }
      return list;
    }

    public void get(int index, RepeatedTrueMapHolder holder) {
      int valueCapacity = getValueCapacity();
      assert index < valueCapacity :
        String.format("Attempted to access index %d when value capacity is %d", index, valueCapacity);

      holder.vector = RepeatedTrueMapVector.this;
      holder.reader = reader;
      holder.start = getOffsetVector().getAccessor().get(index);
      holder.end =  getOffsetVector().getAccessor().get(index + 1);
    }
  }

  public class Mutator extends BaseRepeatedValueVector.BaseRepeatedMutator {

    @Override
    public void startNewValue(int index) {
      emptyPopulator.populate(index + 1);
      offsets.getMutator().setSafe(index + 1, offsets.getAccessor().get(index));
    }

    @Override
    public void setValueCount(int topLevelValueCount) {
      emptyPopulator.populate(topLevelValueCount);
      offsets.getMutator().setValueCount(topLevelValueCount == 0 ? 0 : topLevelValueCount + 1);
      int childValueCount = offsets.getAccessor().get(topLevelValueCount);
      vector.getMutator().setValueCount(childValueCount);
    }

    public int add(int index) {
      int prevEnd = offsets.getAccessor().get(index + 1);
      offsets.getMutator().setSafe(index + 1, prevEnd + 1);
      return prevEnd;
    }
  }
}
