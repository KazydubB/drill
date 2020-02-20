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
import java.util.List;

import org.apache.drill.exec.planner.logical.DrillExceptRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;

// todo: it looks as if this class is not used anewhere (UnionAllPrule is used)
//@Deprecated
public class ExceptDistinctPrule1 extends Prule {

  public static final RelOptRule INSTANCE = new ExceptDistinctPrule1();
  // protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private ExceptDistinctPrule1() {
    super(RelOptHelper.any(DrillExceptRel.class), "Prel.ExceptDistinctPrule1");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    DrillExceptRel except = call.rel(0);
    // return (except.isDistinct() && except.isHomogeneous(false /* don't compare names */));
    return except.isHomogeneous(false /* don't compare names */);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillExceptRel except = call.rel(0);
    final List<RelNode> inputs = except.getInputs();
    List<RelNode> convertedInputList = new ArrayList<>(inputs.size());
    RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL);

    try {
      for (RelNode input : inputs) {
        RelNode convertedInput = convert(input, PrelUtil.fixTraits(call, traits));
        convertedInputList.add(convertedInput);
      }

      traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
      ExceptPrel exceptPrel; // todo: was only distinct
//      ExceptDistinctPrel exceptDistinct =
//          new ExceptDistinctPrel(except.getCluster(), traits, convertedInputList,
//              false /* compatibility already checked during logical phase */);
      if (except.isDistinct()) {
        exceptPrel = new ExceptDistinctPrel(except.getCluster(), traits, convertedInputList, false);
      } else {
        exceptPrel = new ExceptAllPrel(except.getCluster(), traits, convertedInputList, false);
      }

      call.transformTo(exceptPrel);

    } catch (InvalidRelException e) {
      // tracer.warn(e.toString());
    }
  }
}
