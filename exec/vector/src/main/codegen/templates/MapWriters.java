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
<#list ["Single", "Repeated", "True"] as mode>
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
  <#if mode != "True">private final Map<String, FieldWriter> fields = new HashMap<>(); // todo: exclude form TrueMap?</#if>
  <#if mode == "True">
  private TypeProtos.MajorType keyType; // todo: final?
  private TypeProtos.MajorType valueType;
  private FieldWriter keyWriter;
  private FieldWriter valueWriter;
  </#if>
  <#if mode == "Repeated">private int currentChildIndex = 0;</#if>

  private final boolean unionEnabled; // todo: discard for True?

  public ${mode}MapWriter(${containerClass} container, FieldWriter parent,
    boolean unionEnabled<#if mode == "True">, TypeProtos.MajorType keyType, TypeProtos.MajorType valueType</#if>) {
    super(parent);
    this.container = container;
    this.unionEnabled = unionEnabled;
    <#if mode == "True">
    this.keyType = keyType;
    this.valueType = valueType;

    ValueVector keyVector=container.addOrGet("key",keyType, BasicTypeHelper.getValueVectorClass(keyType.getMinorType(),keyType.getMode()));
    ValueVector valueVector=container.addOrGet("value",valueType, BasicTypeHelper.getValueVectorClass(valueType.getMinorType(),valueType.getMode()));
    Class<?> keyWriterClass=BasicTypeHelper.getWriterImpl(keyType.getMinorType(),keyType.getMode());
    Class<?> valueWriterClass=BasicTypeHelper.getWriterImpl(valueType.getMinorType(),valueType.getMode()); // todo: that's not correct way to create writer implementation
    try {
      this.keyWriter = (FieldWriter) keyWriterClass.getDeclaredConstructor(keyVector.getClass(), AbstractFieldWriter.class).newInstance(keyVector, this);
      this.valueWriter = (FieldWriter) valueWriterClass.getDeclaredConstructor(valueVector.getClass(), AbstractFieldWriter.class).newInstance(valueVector, this);
    } catch (Exception e) {
      throw new DrillRuntimeException("Unable to create TrueMapWriter", e);
    }
    //keyWriter.allocate();
    //valueWriter.allocate();
    //keyWriter.setPosition(${index});
    //valueWriter.setPosition(${index});
    </#if>
  }

  public ${mode}MapWriter(${containerClass} container, FieldWriter parent<#if mode == "True">, TypeProtos.MajorType keyType, TypeProtos.MajorType valueType</#if>) {
    this(container, parent, false<#if mode == "True">, keyType, valueType</#if>);
  }

  // todo: evolve
  <#if mode == "True">
// todo: evolve
public void writeKey(ValueHolder holder) {
    // todo: perform key specific operations here
    write(keyWriter, holder);
    }

public void writeValue(ValueHolder holder) {
    // todo: perform key specific operations here
    write(valueWriter, holder);
    }

public void write(FieldWriter writer, ValueHolder holder) {
    //TypeProtos.DataMode mode = keyType.getMode();
    // MinorType type = keyType.getMinorType();
    MinorType type = writer.getField().getType().getMinorType();
    switch (type) {
    case BIGINT:
    writer.write((BigIntHolder) holder);
    break;
    case INT:
        /*switch (mode) {
          case REQUIRED:*/
    writer.write((IntHolder) holder); // todo: actually better to writeInt(((IntHolder) holder).value)
            /*break;
          case OPTIONAL:
            NullableIntHolder nullableHolder = (NullableIntHolder) holder;
            if (nullableHolder.isSet == 1) {
              writer.write(holder);
            } else {
              ((AbstractFieldWriter) writer).writeNull();
            }
            break;
        }*/
    break;
    case VARCHAR:
        /*switch (mode) {
          case REQUIRED:*/
    writer.write((VarCharHolder) holder); // todo: actually better to writeInt(((IntHolder) holder).value)
            /*break;
          case OPTIONAL:
            NullableVarCharHolder nullableHolder = (NullableIntHolder) holder;
            if (nullableHolder.isSet == 1) {
              writer.write(holder);
            } else {
              ((AbstractFieldWriter) writer).writeNull();
            }
            break;
        }*/
    break;
default:
    throw new IllegalArgumentException("Unsupported key type: " + type);
    }
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
  @Override
  public TrueMapWriter trueMap(String name, MajorType keyType, MajorType valueType) {
    <#if mode == "True">
    throw new UnsupportedOperationException("Not yet for True map!");
    <#else>
    /*FieldWriter writer = fields.get(name.toLowerCase());
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
    writer.setPosition(idx());
    fields.put(name.toLowerCase(), writer);
    }
    return writer;*/
    <#if mode != "True">
    // todo: change to FieldWriter?
    TrueMapWriter writer = (TrueMapWriter) fields.get(name.toLowerCase());
    <#else>

    </#if>
    if (writer == null) {
      int vectorCount=container.size();

      TrueMapVector vector = container.addOrGet(name, keyType, valueType);

      writer = new TrueMapWriter(vector, this, keyType, valueType);
      //ValueVector keyVector=container.addOrGet("key",keyType,TypeHelper.getValueVectorClass(keyType.getMinorType(),keyType.getMode()));
      //ValueVector valueVector=container.addOrGet("value",valueType,TypeHelper.getValueVectorClass(valueType.getMinorType(),valueType.getMode()));
      //keyWriterClass=BasicTypeHelper.getWriterImpl(keyType.getMinorType(),keyType.getMode());
      //valueWriterClass=BasicTypeHelper.getWriterImpl(valueType.getMinorType(),valueType.getMode()); // todo: that's not correct way to create writer implementation
      // keyWriter=keyWriterClass.getDeclaredConstructor(keyVector.getClass(),AbstractFieldWriter.class).newInstance(keyVector,this);
      //valueWriter=valueWriterClass.getDeclaredConstructor(valueVector.getClass(),AbstractFieldWriter.class).newInstance(valueVector,this);
      // keyWriter.allocate();
      //valueWriter.allocate();
      //keyWriter.setPosition(${index});
      //valueWriter.setPosition(${index});


      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(${index});
      <#if mode != "True">
      fields.put(name.toLowerCase(), writer);
      <#else>

      </#if>
    }
    return writer; // todo: implement properly!
    </#if>
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
    container.allocateNew();
    <#if mode != "True">
    for(final FieldWriter w : fields.values()) {
      w.allocate();
    }
    <#else>
    keyWriter.allocate();
    valueWriter.allocate();
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
    keyWriter.clear();
    valueWriter.clear();
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
    valueWriter.setPosition(index);
    </#if>
  }

  @Override
  public void start() {
  }

  @Override
  public void end() {
  }

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
