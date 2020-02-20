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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.drill.exec.planner.logical.DrillExceptRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;

public class ExceptAllPrule extends Prule {

  public static final RelOptRule INSTANCE = new ExceptAllPrule();
  // protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private ExceptAllPrule() {
    super(RelOptHelper.any(DrillExceptRel.class), "Prel.ExceptAllPrule");
  }

//  @Override
//  public boolean matches(RelOptRuleCall call) {
//    DrillExceptRel except = call.rel(0);
//    return (!except.isDistinct());
//  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    /*final DrillExceptRel except = call.rel(0);
    final List<RelNode> inputs = except.getInputs();
    List<RelNode> convertedInputList = new ArrayList<>();
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    boolean allHashDistributed = true; // todo: not sure, but I suppose it should be the same as in case of UNION?

    for (RelNode child : inputs) {
      List<DistributionField> childDistFields = new ArrayList<>();
      RelNode convertedChild;

      for (RelDataTypeField f : child.getRowType().getFieldList()) {
        childDistFields.add(new DistributionField(f.getIndex()));
      }

      if (settings.isUnionAllDistributeEnabled()) { // todo: this should be revisited
        *//*
         * Strictly speaking, union-all does not need re-distribution of data; but in Drill's execution
         * model, the data distribution and parallelism operators are the same. Here, we insert a
         * hash distribution operator to allow parallelism to be determined independently for the parent
         * and children. (See DRILL-4833).
         * Note that a round robin distribution would have sufficed but we don't have one.
         *//*
        DrillDistributionTrait hashChild = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(childDistFields));
        RelTraitSet traitsChild = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(hashChild);
        convertedChild = convert(child, PrelUtil.fixTraits(call, traitsChild));
      } else {
        RelTraitSet traitsChild = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL);
        convertedChild = convert(child, PrelUtil.fixTraits(call, traitsChild));
        allHashDistributed = false;
      }
      convertedInputList.add(convertedChild);
    }

    try {
      RelTraitSet traits;
      if (allHashDistributed) { // todo: revisit
        // since all children of union-all are hash distributed, propagate the traits of the left child
        traits = convertedInputList.get(0).getTraitSet();
      } else {
        // output distribution trait is set to ANY since union-all inputs may be distributed in different ways
        // and unlike a join there are no join keys that allow determining how the output would be distributed.
        // Note that a downstream operator may impose a required distribution which would be satisfied by
        // inserting an Exchange after the Union-All.
        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.ANY);
      }

      Preconditions.checkArgument(convertedInputList.size() >= 2, "Except list must contain at least two items."); // todo: should this be exactly 2?
      RelNode left = convertedInputList.get(0);
      for (int i = 1; i < convertedInputList.size(); i++) {
        left = new ExceptAllPrel(except.getCluster(), traits, ImmutableList.of(left, convertedInputList.get(i)),
            false *//* compatibility already checked during logical phase *//*);

      }
      call.transformTo(left);

    } catch (InvalidRelException e) {
      // tracer.warn(e.toString());
    }*/
  }
}
