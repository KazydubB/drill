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
package org.apache.drill.exec.planner.physical;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.planner.common.DrillExceptRelBase;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;

public abstract class ExceptPrel extends DrillExceptRelBase implements Prel {

//  protected final List<NamedExpression> keys = new LinkedList<>();
//  protected final List<NamedExpression> values = new LinkedList<>();

  protected final List<String> leftFields;
  protected final List<String> rightFields;

  protected final List<NamedExpression> leftExpressions;
  protected final List<NamedExpression> rightExpressions;

//  protected final List<Integer> leftKeys;
//  protected final List<Integer> rightKeys;

  public ExceptPrel(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all,
                   boolean checkCompatibility) throws InvalidRelException {
    super(cluster, traits, inputs, all, checkCompatibility);
    leftFields = getInput(0).getRowType().getFieldNames();
    rightFields = getInput(1).getRowType().getFieldNames();

    leftExpressions = new ArrayList<>(leftFields.size());
    for (String field : leftFields) {
      FieldReference fr = FieldReference.getWithQuotedRef(field);
      leftExpressions.add(new NamedExpression(fr, fr));
    }

    rightExpressions = new ArrayList<>(rightFields.size());
    for (String field : rightFields) {
      FieldReference fr = FieldReference.getWithQuotedRef(field);
      rightExpressions.add(new NamedExpression(fr, fr));
    }
//    createKeysAndExprs();
  }

  private void createKeysAndExprs() {
//    final List<String> childFields = getInput(1).getRowType().getFieldNames();
//    final List<String> fields = getInput(0).getRowType().getFieldNames();
//
//    for (String childField : childFields) { // todo: aggr calls?
//      FieldReference fr = FieldReference.getWithQuotedRef(childField);
//      keys.add(new NamedExpression(fr, fr));
//    }
//
//    for (String field : fields) { // todo: should differentiate the source tables for the keys and values
//      FieldReference fr = FieldReference.getWithQuotedRef(field);
//      values.add(new NamedExpression(fr, fr));
//    }

    // todo: aggregate calls see in AggPrelBase
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(this.getInputs());
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false; // todo: not sure
  }

  public List<String> getLeftFields() {
    return leftFields;
  }

  public List<String> getRightFields() {
    return rightFields;
  }

  public List<NamedExpression> getLeftExpressions() {
    return leftExpressions;
  }

  public List<NamedExpression> getRightExpressions() {
    return rightExpressions;
  }

//  public List<NamedExpression> getKeys() {
//    return keys;
//  }
//
//  public List<NamedExpression> getValues() {
//    return values;
//  }
}
