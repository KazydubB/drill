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
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalMinus;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalMinus} to a Drill EXCEPT operation.
 */
@Deprecated
public class DrillExceptRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new DrillExceptRule();

  private DrillExceptRule() {
    super(RelOptHelper.any(LogicalMinus.class, Convention.NONE),
        DrillRelFactories.LOGICAL_BUILDER, "DrillExceptRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalMinus minus = call.rel(0);

    // This rule applies to except (distinct) only
//    if (minus.all) {
//      return;
//    }

//    System.out.println("Hello from DrillExceptRule");

    final RelTraitSet traits = minus.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    final List<RelNode> convertedInputs = new ArrayList<>();
    for (RelNode input : minus.getInputs()) {
      final RelNode convertedInput = convert(input, input.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());
      convertedInputs.add(convertedInput);
    }

    try {
      call.transformTo(new DrillExceptRel(minus.getCluster(), traits, convertedInputs, minus.all, true));
    } catch (InvalidRelException e) {
      // tracer.warn(e.toString());
      System.out.println(String.format("failed to transform to DrillExceptRel: %s", e.getMessage()));
    }
  }
}
