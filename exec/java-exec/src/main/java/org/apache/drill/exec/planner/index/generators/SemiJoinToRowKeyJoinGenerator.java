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
package org.apache.drill.exec.planner.index.generators;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.index.FlattenPhysicalPlanCallContext;
import org.apache.drill.exec.planner.index.FunctionalIndexInfo;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.index.generators.common.SemiJoinIndexPlanUtils;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillSemiJoinRel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.physical.Prule;

import java.util.List;

import static org.apache.drill.exec.planner.physical.Prel.DRILL_PHYSICAL;


/**
 * Generate a covering index plan that is equivalent to the original plan.
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push this filter into the index scan.
 */
public class SemiJoinToRowKeyJoinGenerator extends CoveringIndexPlanGenerator {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinToRowKeyJoinGenerator.class);
  private final SemiJoinIndexPlanCallContext joinContext;

  public SemiJoinToRowKeyJoinGenerator(SemiJoinIndexPlanCallContext indexContext,
                                       FunctionalIndexInfo functionInfo,
                                       IndexGroupScan indexGroupScan,
                                       RexNode indexCondition,
                                       RexNode remainderCondition,
                                       RexBuilder builder,
                                       PlannerSettings settings) {
    super(indexContext.rightSide, functionInfo, indexGroupScan,
            indexCondition, remainderCondition, builder, settings);
    this.joinContext = indexContext;
  }

  @Override
  public List<RelNode> convertChildMulti(RelNode join, final RelNode input) throws InvalidRelException {
    RelNode nonCoveringIndexPlan = super.convertChild(joinContext.rightSide.upperProject, input);

    if (nonCoveringIndexPlan == null) {
      logger.info("semi_join_index_plan_info: Non covering index plan(base class) is null.");
      throw new InvalidRelException("Non-covering index plan generated by base class is null");
    }

    FlattenPhysicalPlanCallContext leftSideJoinContext = joinContext.getLeftFlattenPhysicalPlanCallContext();

    //build HashAgg for distinct processing
    List<RelNode> agg = SemiJoinIndexPlanUtils.buildAgg(joinContext, joinContext.distinct, nonCoveringIndexPlan);
    logger.debug("semi_join_index_plan_info: generated hash aggregation operators: {}", agg);

    return SemiJoinIndexPlanUtils.buildRowKeyJoin(joinContext,leftSideJoinContext.getRoot(), buildRangePartitioners(agg));
  }

  @Override
  public boolean goMulti() throws InvalidRelException {
    RelNode top = indexContext.getCall().rel(0);
    if (top instanceof DrillJoinRel) {
      DrillJoinRel join = (DrillJoinRel) top;
      final RelNode input0 = join.getInput(0);
      RelTraitSet traits0 = input0.getTraitSet().plus(DRILL_PHYSICAL);
      RelNode convertedInput0 = Prule.convert(input0, traits0);
      return this.goMulti(top, convertedInput0);
    } else if (top instanceof DrillSemiJoinRel) {
      DrillSemiJoinRel join = (DrillSemiJoinRel) top;
      final RelNode input0 = join.getInput(0);
      RelTraitSet traits0 = input0.getTraitSet().plus(DRILL_PHYSICAL);
      RelNode convertedInput0 = Prule.convert(input0, traits0);
      return this.goMulti(top, convertedInput0);
    } else {
        return false;
    }
  }
}