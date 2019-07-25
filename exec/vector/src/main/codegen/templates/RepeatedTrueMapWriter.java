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

import org.apache.drill.exec.vector.complex.writer.FieldWriter;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/RepeatedTrueMapWriter.java" />

<#include "/@includes/license.ftl" />
package org.apache.drill.exec.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
public class RepeatedTrueMapWriter extends AbstractFieldWriter implements BaseWriter.TrueMapWriter {

  final RepeatedTrueMapVector container;

  private final SingleTrueMapWriter trueMapWriter;
  private int currentChildIndex;

  RepeatedTrueMapWriter(RepeatedTrueMapVector container, FieldWriter parent) {
    super(parent);
    this.container = Preconditions.checkNotNull(container, "Container cannot be null!");
    this.trueMapWriter = new SingleTrueMapWriter((TrueMapVector) container.getDataVector(), this);
  }

  @Override
  public void allocate() {
    container.allocateNew();
  }

  @Override
  public void clear() {
    container.clear();
  }

  @Override
  public void close() {
    clear();
    container.close();
  }

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  public void setValueCount(int count){
    container.getMutator().setValueCount(count);
  }

  @Override
  public void startList() {
    // make sure that the current vector can support the end position of this list.
    if (container.getValueCapacity() <= idx()) {
      container.getMutator().setValueCount(idx() + 1);
    }

    // update the repeated vector to state that there is current+1 objects.
    final RepeatedTrueMapHolder h = new RepeatedTrueMapHolder();
    container.getAccessor().get(idx(), h);
    if (h.start >= h.end) {
      container.getMutator().startNewValue(idx());
    }
    currentChildIndex = container.getOffsetVector().getAccessor().get(idx());
  }

  @Override
  public void endList() {
    // noop, we initialize state at start rather than end.
  }

  @Override
  public MaterializedField getField() {
    return container.getField();
  }

  @Override
  public void start() {
    currentChildIndex = container.getMutator().add(idx());
    trueMapWriter.setPosition(currentChildIndex);
    trueMapWriter.start();
  }

  @Override
  public void end() {
    trueMapWriter.end();
  }

  @Override
  public void startKeyValuePair() {
    trueMapWriter.startKeyValuePair();
  }

  @Override
  public void endKeyValuePair() {
    trueMapWriter.endKeyValuePair();
  }

  @Override
  public ListWriter list(String name) {
    ListWriter writer = trueMapWriter.list(name);
    writer.setPosition(currentChildIndex);
    return writer;
  }

  @Override
  public MapWriter map(String name) {
    MapWriter writer = trueMapWriter.map(name);
    writer.setPosition(currentChildIndex);
    return writer;
  }

  @Override
  public TrueMapWriter trueMap(String name) {
    TrueMapWriter writer = trueMapWriter.trueMap(name);
    writer.setPosition(currentChildIndex);
    return writer;
  }

  <#list vv.types as type>
    <#list type.minor as minor>
      <#assign lowerName = minor.class?uncap_first />
      <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>

  @Override
  public ${minor.class}Writer ${lowerName}(String name) {
    FieldWriter writer = (FieldWriter) trueMapWriter.${lowerName}(name);
    writer.setPosition(currentChildIndex);
    return writer;
  }
      <#if minor.class?contains("Decimal") >

  @Override
  public ${minor.class}Writer ${lowerName}(String name, int scale, int precision) {
    FieldWriter writer = (FieldWriter) trueMapWriter.${lowerName}(name, scale, precision);
    writer.setPosition(currentChildIndex);
    return writer;
  }
      </#if>
    </#list>
  </#list>
}
