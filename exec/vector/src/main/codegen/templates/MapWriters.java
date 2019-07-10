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
<#list ["Single", "Repeated"] as mode> <#-- remove 'True' -->
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/${mode}MapWriter.java" />
<#if mode == "Single">
<#assign containerClass = "MapVector" />
<#assign index = "idx()">
<#elseif mode == "Repeated">
<#assign containerClass = "RepeatedMapVector" />
<#assign index = "currentChildIndex">
<#elseif mode == "True"> // todo: remove it from here and extend RepeatedMapWriter instead
<#assign containerClass = "TrueMapVector" />
<#assign index = "idx()"> // todo: change to currentRow, and set it to 0 default
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
// todo: move TrueMapWriter to a separate FreeMarker class; extend RepeatedMapWriter?
// todo: SingleMapWriter for TrueMapWriter
public class ${mode}MapWriter extends AbstractFieldWriter {

  protected final ${containerClass} container;
  <#if mode == "Repeated">protected<#else>private</#if> final Map<String, FieldWriter> fields = new HashMap<>();
// todo: change to int lastSet
  <#if mode == "True">
  private int currentRow = -1;
  private int length = -1; // (designates length for current row)
  private boolean rowStarted;
  </#if>
  <#if mode == "Repeated">protected int currentChildIndex = 0;</#if>

  private final boolean unionEnabled; // todo: discard for True? Probably yes

  public ${mode}MapWriter(${containerClass} container, FieldWriter parent, boolean unionEnabled) {
    super(parent);
    this.container = container;
    this.unionEnabled = unionEnabled;
  }

  public ${mode}MapWriter(${containerClass} container, FieldWriter parent) {
    this(container, parent, false);
  }

  <#if mode == "True">
  public void startKeyValuePair() {
    // todo: should entry be marked as not-set (null) explicitly?
    int idx = idx();
//    super.setPosition(currentRow);
    int index = getPosition();
    assert rowStarted : "Must start row (start()) before put";
    for (FieldWriter writer : fields.values()) {
    writer.setPosition(index); // todo: variable
    }
    // setPosition(getPosition()); // todo: remove?
  }

// todo: discard
  @Deprecated
  private int getPosition() { // todo: rename to index?
    assert rowStarted : "Must start row (start()) before getPosition()";
    // todo: change currentRow with idx()?
    int offset = container.getInnerOffset(currentRow); // todo: there can be problems connected to this
    return offset + length;
  }

  public void endKeyValuePair() {
    assert rowStarted : "Must start row (start()) before incrementing current length";
    length++;
  }
  </#if>

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  @Override
  public boolean isEmptyMap() {
    return container.size() == 0;
  }

  @Override
  public MaterializedField getField() {
      return container.getField();
  }

  @Override
  public MapWriter map(String name) {
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
  }

  // todo: change this!?
  @Override // todo: TrueMapWriter should support his operation. As well as others map(), list() and list(String name)
  public TrueMapWriter trueMap(String name, MajorType keyType, MajorType valueType) {
    <#if mode == "True">
    assert TrueMapVector.FIELD_VALUE_NAME.equals(name) : "Only value field is allowed in TrueMap";
    // todo: change to FieldWriter?
    </#if>
    TrueMapWriter writer = (TrueMapWriter) fields.get(name.toLowerCase());
    if (writer == null) {
      int vectorCount=container.size();

      // todo: this one... probably reimplement addOrGet
      TrueMapVector vector = container.addOrGet(name, TrueMapVector.TYPE, TrueMapVector.class);
      vector.setKeyValueTypes(keyType, valueType);

      writer = new TrueMapWriter(vector, this);

      fields.put(name.toLowerCase(), writer);
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
    return fields.get(TrueMapVector.FIELD_KEY_NAME);
  }

  @Override
  public FieldWriter getValueWriter() {
    return fields.get(TrueMapVector.FIELD_VALUE_NAME);
  }
  </#if>

  @Override
  public void close() throws Exception {
    clear();
    container.close();
  }

  @Override
  public void allocate() {
    container.allocateNew();
    for(final FieldWriter w : fields.values()) {
      w.allocate();
    }
  }

  @Override
  public void clear() {
    container.clear();
    for(final FieldWriter w : fields.values()) {
      w.clear();
    }
  }

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
  <#if mode == "Repeated">

  public void start() {
      // update the repeated vector to state that there is current+1 objects.
    final ${mode}MapHolder h = new ${mode}MapHolder();
    final ${mode}MapVector map = container;
    final ${mode}MapVector.Mutator mutator = map.getMutator();

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
    // todo: remove?
//    if (fields.size() == TrueMapVector.NUMBER_OF_CHILDREN) {
//      getKeyWriter().setPosition(index);
//      getValueWriter().setPosition(index);
//    }
    </#if>
  }

  @Override
  public void start() {
  <#if mode == "True"> // todo: remove
    currentRow++; // todo: currentRow = idx();
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
//    if (currentRow > 0) {
    currentOffset += offset;//container.getInnerOffset(currentRow);
//    } // todo: decide if this should be done for currentRow + 1 or not
    container.setInnerOffset(currentRow + 1, currentOffset);
  </#if>
  }

  </#if>

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
    getKeyWriter().setPosition(index);
    getValueWriter().setPosition(index);
    }

public MajorType getKeyType() {
    return container.getKeyType();
    }

public MajorType getValueType() {
    return container.getValueType();
    }
</#if>

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
  // todo: make optional/required variants?
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
}
</#list>
