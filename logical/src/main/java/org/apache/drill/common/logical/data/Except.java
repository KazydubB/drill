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
package org.apache.drill.common.logical.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("except")
public class Except extends LogicalOperatorBase {

  private final List<LogicalOperator> inputs;
  private final boolean distinct; // todo: change to all to be consistent with other code in DrillExceptRel?

  @JsonCreator
  public Except(@JsonProperty("inputs") List<LogicalOperator> inputs, @JsonProperty("distinct") Boolean distinct) { // todo: probably pass all?
    this.inputs = inputs;
    for (LogicalOperator o : inputs) {
      o.registerAsSubscriber(this);
    }
    this.distinct = distinct == null ? false : distinct;
  }

  public List<LogicalOperator> getInputs() {
    return inputs;
  }

  public boolean isDistinct() {
    return distinct;
  }

  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitExcept(this, value);
  }

  @Override
  public Iterator<LogicalOperator> iterator() {
    return inputs.iterator();
  }


  public static Builder builder(){
    return new Builder();
  }

  public static class Builder extends AbstractBuilder<Except> {

    private List<LogicalOperator> inputs = new ArrayList<>();
    private boolean distinct;

    public Builder addInput(LogicalOperator op) {
      inputs.add(op);
      return this;
    }

    public Builder setDistinct(boolean distinct){
      this.distinct = distinct;
      return this;
    }

    @Override
    public Except build() {
      return new Except(inputs, distinct);
    }
  }
}
