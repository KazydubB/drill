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
<@pp.dropOutputFile />
<#list ["Single", "Repeated", "True"] as mode> <#-- remove 'True' -->
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/${mode}MapWriter.java" />
<#if mode == "Single">
<#assign containerClass = "MapVector" />
<#assign index = "idx()">
<#elseif mode == "Repeated">
<#assign containerClass = "RepeatedMapVector" />
<#assign index = "currentChildIndex">
<#elseif mode == "True">
<#assign containerClass = "TrueMapVector" />
<#assign index = "idx()"> // todo: not sure
</#if>

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
import java.util.Map;
import java.util.HashMap;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.expr.holders.RepeatedMapHolder;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;
<#if mode == "True">
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.BasicTypeHelper;
</#if>

/*
 * This class is generated using FreeMarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class ${mode}MapWriter extends AbstractFieldWriter {

  protected final ${containerClass} container;
  <#--<#if mode != "True">-->
  private final Map<String, FieldWriter> fields = new HashMap<>();
<#if mode == "True">
  private int currentRow = -1;
  private int length = -1; // (designates length for current row)
  private boolean rowStarted;
  // todo: probably remove
  private TypeProtos.MajorType keyType; // todo: final?
  private TypeProtos.MajorType valueType;
  private FieldWriter keyWriter;
  private FieldWriter valueWriter;
  // shows if allocated
  private boolean initialized;
  </#if>
  <#if mode == "Repeated">private int currentChildIndex = 0;</#if>

  private final boolean unionEnabled; // todo: discard for True?

  public ${mode}MapWriter(${containerClass} container, FieldWriter parent,
    // todo: actually remove keyType and valueType arguments as these are contained in TrueMap container
    boolean unionEnabled<#if mode == "True">, TypeProtos.MajorType keyType, TypeProtos.MajorType valueType</#if>) {
    // boolean unionEnabled) {
    super(parent);
    this.container = container;
    this.unionEnabled = unionEnabled;
    <#if mode == "True">
//    this.keyType = container.getKeyType();
//    this.valueType = container.getValueType();
    this.keyType = keyType;
    this.valueType = valueType;
    if (keyType != null && valueType != null) {
      init();
    }
    // todo: change String literals to defined (in TrueMapVector) constants
//    ValueVector keyVector=container.getChild(TrueMapVector.FIELD_KEY_NAME);
//    ValueVector valueVector=container.getChild(TrueMapVector.FIELD_VALUE_NAME);
//    Class<?> keyWriterClass=BasicTypeHelper.getWriterImpl(keyType.getMinorType(),keyType.getMode());
//    Class<?> valueWriterClass = null;
//    if (valueType.getMinorType() != MinorType.TRUEMAP) {
//      valueWriterClass=BasicTypeHelper.getWriterImpl(valueType.getMinorType(),valueType.getMode()); // todo: that's not correct way to create writer implementation
//    }
//    try {
//      this.keyWriter = (FieldWriter) keyWriterClass.getDeclaredConstructor(keyVector.getClass(), AbstractFieldWriter.class).newInstance(keyVector, this);
//      if (valueWriterClass != null) {
//        this.valueWriter = (FieldWriter) valueWriterClass.getDeclaredConstructor(
//          valueVector.getClass(), valueType.getMinorType() == TypeProtos.MinorType.MAP ? FieldWriter.class : AbstractFieldWriter.class).newInstance(valueVector, this);
//        initialized = true;
//      }
//    } catch (Exception e) {
//      throw new DrillRuntimeException("Unable to create TrueMapWriter", e);
//    }
    </#if>
  }

  <#if mode == "True">
    public void init() {
    ValueVector keyVector=container.getChild(TrueMapVector.FIELD_KEY_NAME);
    ValueVector valueVector=container.getChild(TrueMapVector.FIELD_VALUE_NAME);
      Class<?> keyWriterClass=BasicTypeHelper.getWriterImpl(keyType.getMinorType(),keyType.getMode());
      Class<?> valueWriterClass = null;
      if (valueType.getMinorType() != MinorType.TRUEMAP) {
        valueWriterClass=BasicTypeHelper.getWriterImpl(valueType.getMinorType(),valueType.getMode()); // todo: that's not correct way to create writer implementation
      }
      try {
        this.keyWriter = (FieldWriter) keyWriterClass.getDeclaredConstructor(keyVector.getClass(), AbstractFieldWriter.class).newInstance(keyVector, this);
//        if (valueWriterClass != null) {
          this.valueWriter = (FieldWriter) valueWriterClass.getDeclaredConstructor(
            valueVector.getClass(), valueType.getMinorType() == TypeProtos.MinorType.MAP ? FieldWriter.class : AbstractFieldWriter.class).newInstance(valueVector, this);
          initialized = true;
//        }
    fields.put(TrueMapVector.FIELD_KEY_NAME, keyWriter);
    fields.put(TrueMapVector.FIELD_VALUE_NAME, valueWriter);
      } catch (Exception e) {
        throw new DrillRuntimeException("Unable to create TrueMapWriter", e);
      }
    }
  </#if>

  public ${mode}MapWriter(${containerClass} container, FieldWriter parent<#if mode == "True">, TypeProtos.MajorType keyType, TypeProtos.MajorType valueType</#if>) {
    this(container, parent, false<#if mode == "True">, keyType, valueType</#if>);
  }

  <#if mode == "True">
  public void startKeyValuePair() {
    assert rowStarted : "Must start row (start()) before put";
    setPosition(getPosition());
  }

  private int getPosition() { // todo: rename to index?
    assert rowStarted : "Must start row (start()) before getPosition()";
    int offset = container.getInnerOffset(currentRow); // todo: there can be problems connected to this
    return offset + length;
  }

  public void endKeyValuePair() {
    assert rowStarted : "Must start row (start()) before incrementing current length";
    length++;
  }

public void allocateValueWriter() {
    ValueVector valueVector=container.getChild(TrueMapVector.FIELD_VALUE_NAME);
    Class<?> valueWriterClass = valueWriterClass=BasicTypeHelper.getWriterImpl(valueType.getMinorType(),valueType.getMode());
    try {
    // this.valueWriter = (FieldWriter) valueWriterClass.getDeclaredConstructor(
    // valueVector.getClass(), valueType.getMinorType() == MinorType.MAP ? FieldWriter.class : AbstractFieldWriter.class).newInstance(valueVector, this);
    switch (valueType.getMinorType()) {
    case MAP:
    valueWriter = (FieldWriter) valueWriterClass.getDeclaredConstructor(valueVector.getClass(), FieldWriter.class)
    .newInstance(valueVector, this);
    break;
    case TRUEMAP:
//        this.valueWriter = (FieldWriter) valueWriterClass.getDeclaredConstructor(valueVector.getClass(), FieldWriter.class, MajorType.class, MajorType.class) // todo: container type can be hardcoded
//            .newInstance(valueVector, this, ((TrueMapVector) valueVector).getKeyType(), ((TrueMapVector) valueVector).getValueType());
    this.valueWriter = ((TrueMapVector) valueVector).getWriter();
    break;
default:
    this.valueWriter = (FieldWriter) valueWriterClass.getDeclaredConstructor(valueVector.getClass(), AbstractFieldWriter.class).newInstance(valueVector, this);
    break;
    }
    } catch (Exception e) {
    throw new DrillRuntimeException("Unable to create TrueMapWriter", e);
    }
    initialized = true;
    }
  </#if>

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  @Override
  public boolean isEmptyMap() {
    return 0 == container.size();
  }

  @Override
  public MaterializedField getField() {
      return container.getField();
  }

  @Override
  public MapWriter map(String name) {
    <#if mode != "True">
      FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null){
      int vectorCount=container.size();
        MapVector vector = container.addOrGet(name, MapVector.TYPE, MapVector.class);
      if(!unionEnabled){
        writer = new SingleMapWriter(vector, this);
      } else {
        writer = new PromotableWriter(vector, container);
      }
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(${index});
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
    <#else>
    throw new UnsupportedOperationException("Not for true map!");
    </#if>
  }

  // todo: change this!?
  @Override // todo: TrueMapWriter should support his operation. As well as others map(), list() and list(String name)
  public TrueMapWriter trueMap(String name, MajorType keyType, MajorType valueType) {
    <#if mode != "True">
    // todo: change to FieldWriter?
    TrueMapWriter writer = (TrueMapWriter) fields.get(name.toLowerCase()); // todo: ?
    <#else>
    assert TrueMapVector.FIELD_VALUE_NAME.equals(name) : "Only value field is allowed in TrueMap";
    TrueMapWriter writer = null;
    </#if>
    if (writer == null) {
      int vectorCount=container.size();

      // todo: this one... probably reimplement addOrGet
    // todo: is keyType/valueType actually required here?
      TrueMapVector vector = container.addOrGet(name, <#if mode == "True">TrueMapVector.TYPE, </#if>keyType, valueType); // todo: in case if this is TrueMap name should be "value"

      writer = new TrueMapWriter(vector, this, keyType, valueType);
      // todo: remove
      vector.setWriter(writer);

  <#if mode != "True">
    fields.put(name.toLowerCase(), writer);
  <#else>
    // todo: add logic for TrueMap!
  </#if>
      if (valueType.getMinorType() == MinorType.TRUEMAP) { // todo: remove?
        return writer;
      }
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(${index});
    }
    return writer; // todo: implement properly!
  }

  <#if mode == "True">
  @Override
  public FieldWriter getKeyWriter() {
    return keyWriter;
  }

  @Override
  public FieldWriter getValueWriter() {
    return valueWriter;
  }
  </#if>

  @Override
  public void close() throws Exception {
    clear();
    container.close();
  }

  @Override
  public void allocate() {
    <#if mode != "True">
    container.allocateNew();
    for(final FieldWriter w : fields.values()) {
      w.allocate();
    }
    <#else>
//    keyWriter.allocate();
//    valueWriter.allocate();
//    if (keyWriter != null) {
//      keyWriter.allocate();
//    }
//    if (valueWriter != null) {
//      valueWriter.allocate();
//    }
//    initialized = container.size() == TrueMapVector.NUMBER_OF_CHILDREN;
    container.getDataVector().allocateNew();
    for (FieldWriter child : fields.values()) {
    child.allocate();
    }
    </#if>
  }

  @Override
  public void clear() {
    container.clear();
    <#if mode != "True">
    for(final FieldWriter w : fields.values()) {
      w.clear();
    }
    <#else>
//    keyWriter.clear();
//    valueWriter.clear();
    if (keyWriter != null) {
      keyWriter.clear();
    }
    if (valueWriter != null) {
      valueWriter.clear();
    }
    </#if>
  }

  <#if mode != "True">
// todo: UnsupportedOpException for True?
  @Override
  public ListWriter list(String name) {
    FieldWriter writer = fields.get(name.toLowerCase());
    int vectorCount = container.size();
    if(writer == null) {
      if (!unionEnabled){
        writer = new SingleListWriter(name,container,this);
      } else{
        writer = new PromotableWriter(container.addOrGet(name, Types.optional(MinorType.LIST), ListVector.class), container);
      }
      if (container.size() > vectorCount) {
        writer.allocate();
      }
      writer.setPosition(${index});
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }
  </#if>
  <#if mode == "Repeated">
  public void start() {
      // update the repeated vector to state that there is current+1 objects.
    final RepeatedMapHolder h = new RepeatedMapHolder();
    final RepeatedMapVector map = (RepeatedMapVector) container;
    final RepeatedMapVector.Mutator mutator = map.getMutator();

    // Make sure that the current vector can support the end position of this list.
    if(container.getValueCapacity() <= idx()) {
      mutator.setValueCount(idx()+1);
    }

    map.getAccessor().get(idx(), h);
    if (h.start >= h.end) {
      container.getMutator().startNewValue(idx());
    }
    currentChildIndex = container.getMutator().add(idx());
    for(final FieldWriter w : fields.values()) {
      w.setPosition(currentChildIndex);
    }
  }


  public void end() {
    // noop
  }
  <#else>

  public void setValueCount(int count) {
    container.getMutator().setValueCount(count);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
<#if mode != "True">
    for(final FieldWriter w: fields.values()) {
      w.setPosition(index);
    }
    <#else>
    keyWriter.setPosition(index);
    if (valueWriter == null) { // todo: solve this normally
      allocateValueWriter();
    }
    valueWriter.setPosition(index);
    </#if>
  }

  @Override
  public void start() {
  <#if mode == "True">
    currentRow++;
    length = 0;
    rowStarted = true;
  </#if>
  }

  @Override
  public void end() {
  <#if mode == "True">
    assert rowStarted : "Must start row (start()) before end()";
    int offset = container.getInnerOffset(currentRow); // todo: this may be not true in a case when currentRow is
    rowStarted = false;
    int currentOffset = length;
    if (currentRow > 0) {
    currentOffset += container.getInnerOffset(currentRow);
    } // todo: decide if this should be done for currentRow + 1 or not
    container.setInnerOffset(currentRow + 1, currentOffset);
  </#if>
  }

  <#if mode == "True">
  public void put(int outputIndex, Object key, Object value) { // todo: outputIndex?
    assert rowStarted : "Must start row (startRow()) before put";

    int index = getPosition();
    // todo: change this?
    setValue(container.getKeys(), key, index);
    setValue(container.getValues(), value, index);
    length++;
  }

  private void setValue(ValueVector vector, Object value, int index) {
    if (vector instanceof NullableIntVector) { // todo: not instanceof but type?
      ((NullableIntVector) vector).getMutator().setSafe(index, (int) value);
    } else if (vector instanceof NullableVarCharVector) {
      byte[] bytes = (byte[]) value;
      ((NullableVarCharVector) vector).getMutator().setSafe(index, bytes, 0, bytes.length);
    }
  }

  public void put(int outputIndex, ValueHolder keyHolder, ValueHolder valueHolder) { // todo: outputIndex?
    assert rowStarted : "Must start row (startRow()) before put";

    int index = getPosition();
    keyWriter.setPosition(index);
    valueWriter.setPosition(index);
  }

  public MajorType getKeyType() {
    return keyType;
  }

  public MajorType getValueType() {
    return valueType;
  }
  </#if>

  </#if>

<#if mode != "True">
  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#assign vectName = capName />
  <#assign vectName = "Nullable${capName}" />

  <#if minor.class?contains("Decimal") >
  @Override
  public ${minor.class}Writer ${lowerName}(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(name.toLowerCase());
    assert writer != null;
    return writer;
  }

  @Override
  public ${minor.class}Writer ${lowerName}(String name, int scale, int precision) {
    final MajorType ${upperName}_TYPE = Types.withScaleAndPrecision(MinorType.${upperName}, DataMode.OPTIONAL, scale, precision);
  <#else>
  private static final MajorType ${upperName}_TYPE = Types.optional(MinorType.${upperName});
  @Override
  public ${minor.class}Writer ${lowerName}(String name) {
  </#if>
    FieldWriter writer = fields.get(name.toLowerCase());
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      if (unionEnabled){
        ${vectName}Vector v = container.addOrGet(name, ${upperName}_TYPE, ${vectName}Vector.class);
        writer = new PromotableWriter(v, container);
        vector = v;
      } else {
        ${vectName}Vector v = container.addOrGet(name, ${upperName}_TYPE, ${vectName}Vector.class);
        writer = new ${vectName}WriterImpl(v, this);
        vector = v;
      }
      if (currentVector == null || currentVector != vector) {
        vector.allocateNewSafe();
      } 
      writer.setPosition(${index});
      fields.put(name.toLowerCase(), writer);
    }
    return writer;
  }

  </#list></#list>
</#if>
}
</#list>
