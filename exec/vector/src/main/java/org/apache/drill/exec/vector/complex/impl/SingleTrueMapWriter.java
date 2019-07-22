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
package org.apache.drill.exec.vector.complex.impl;

import org.apache.drill.exec.expr.holders.TrueMapHolder;
import org.apache.drill.exec.vector.complex.TrueMapVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

public class SingleTrueMapWriter extends RepeatedMapWriter implements BaseWriter.TrueMapWriter {

  final TrueMapVector container;
  private boolean mapStarted;

  public SingleTrueMapWriter(TrueMapVector container, FieldWriter parent, boolean unionEnabled) {
    super(container, parent, unionEnabled);
    this.container = container;
  }

  public SingleTrueMapWriter(TrueMapVector container, FieldWriter parent) {
    this(container, parent, false);
  }

  @Override
  public void startKeyValuePair() {
    currentChildIndex = container.getMutator().add(idx());
    for (FieldWriter w : fields.values()) {
      w.setPosition(currentChildIndex);
    }
  }

  @Override
  public void endKeyValuePair() {
    checkStarted();
  }

  @Override
  public TrueMapWriter trueMap(String name) {
    // todo: consider the same assertion for list(String), list()??, map(String) methods
    assert TrueMapVector.FIELD_VALUE_NAME.equals(name) : "Only value field is allowed in TrueMap. Found: " + name;
    return super.trueMap(name);
  }

  // todo: remove!
  @Override
  public FieldWriter getKeyWriter() {
    return fields.get(TrueMapVector.FIELD_KEY_NAME);
  }
  // todo: remove!
  @Override
  public FieldWriter getValueWriter() {
    return fields.get(TrueMapVector.FIELD_VALUE_NAME);
  }

  public void setValueCount(int count) {
    container.getMutator().setValueCount(count);
  }

  @Override
  public void start() {
    assert !mapStarted : "Map should not be started";

    // Make sure that the current vector can support the end position of this list.
    if (container.getValueCapacity() <= idx()) {
      container.getMutator().setValueCount(idx() + 1);
    }

    TrueMapHolder h = new TrueMapHolder();
    container.getAccessor().get(idx(), h);
    if (h.start >= h.end) {
      container.getMutator().startNewValue(idx());
    }

    mapStarted = true;
  }

  @Override
  public void end() {
    checkStarted();
    mapStarted = false;
  }

  private void checkStarted() {
    assert mapStarted : "Must start map (startRow()) before";
  }
}
