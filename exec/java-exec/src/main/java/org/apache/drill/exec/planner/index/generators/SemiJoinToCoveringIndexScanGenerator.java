/**
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.index.FunctionalIndexInfo;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.index.generators.common.SemiJoinIndexPlanUtils;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.physical.Prule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;
import static org.apache.drill.exec.planner.physical.Prel.DRILL_PHYSICAL;

/**
 * Generate a covering index plan that is equivalent to the original plan.
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push this filter into the index scan.
 */
public class SemiJoinToCoveringIndexScanGenerator extends CoveringIndexPlanGenerator {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinToCoveringIndexScanGenerator.class);
  private final SemiJoinIndexPlanCallContext joinContext;

  public SemiJoinToCoveringIndexScanGenerator(SemiJoinIndexPlanCallContext indexContext,
                                              FunctionalIndexInfo functionInfo,
                                              IndexGroupScan indexGroupScan,
                                              RexNode indexCondition,
                                              RexNode remainderCondition,
                                              RexBuilder builder,
                                              PlannerSettings settings) {
    super(indexContext.getConveringIndexContext(), functionInfo, indexGroupScan,
          indexCondition, remainderCondition, builder, settings);
    this.joinContext = indexContext;
  }

  @Override
  public RelNode convertChild(final RelNode join, final RelNode input) throws InvalidRelException {
    Preconditions.checkNotNull(joinContext.getConveringIndexContext());
    RelNode coveringIndexScanRel = super.convertChild(joinContext.getConveringIndexContext().upperProject, input);
    if (coveringIndexScanRel == null) {
      logger.info("semi_join_index_plan_info: Covering index plan(base class) is null.");
      throw new InvalidRelException("Covering index plan generated by base class is null");
    }

    Map<Integer, Integer> inputOutputRefMap = new HashMap<>();
    Pair<ImmutableBitSet, List<ImmutableBitSet>> grpSets = createGroupSets(inputOutputRefMap, 0);
    List<AggregateCall> aggregateCalls = createAggregateCalls(inputOutputRefMap, 0 + joinContext.join.getRightKeys().size());

    //build the Drill logical Aggregate with _id as distinct and other columns as min(colname).
    DrillAggregateRel aggregateRel = new DrillAggregateRel(input.getCluster(), input.getTraitSet().plus(DRILL_LOGICAL),
            coveringIndexScanRel, false, grpSets.left, grpSets.right, aggregateCalls);
    //build HashAgg for distinct processing
    List<RelNode> agg = SemiJoinIndexPlanUtils.buildAgg(joinContext, aggregateRel, coveringIndexScanRel);

    //build project to match the output rowType.
    List<RexNode> exprs = IndexPlanUtils.projects(agg.get(0), agg.get(0).getRowType().getFieldNames());
    List<RexNode> projectExprs = rearrange(exprs, inputOutputRefMap);
    return new ProjectPrel(input.getCluster(), input.getTraitSet().plus(DRILL_PHYSICAL), agg.get(0),
                          projectExprs, joinContext.join.getRowType());
  }

  private List<RexNode> rearrange(List<RexNode> exprs, Map<Integer, Integer> refMap) {
    List<RexNode> result = new ArrayList<>();

    for (int i=0; i< exprs.size(); i++) {
      result.add(exprs.get(refMap.get(i)));
    }
    return result;
  }

  private Pair<ImmutableBitSet, List<ImmutableBitSet>> createGroupSets(Map<Integer, Integer> refMap, int outStartIndex) {
    List<Integer> newGrpCols = Lists.newArrayList();
    int index = joinContext.join.getInput(0).getRowType().getFieldCount();
    int outIndex = outStartIndex;
    for (int groupCol : joinContext.join.getRightKeys()) {
      refMap.put(index + groupCol, outIndex++);
      newGrpCols.add(index + groupCol);
    }
    ImmutableBitSet grpSet = ImmutableBitSet.of(newGrpCols);
    List<ImmutableBitSet> grpSets = Lists.newArrayList();
    grpSets.add(grpSet);
    return Pair.of(grpSet, grpSets);
  }

  private List<AggregateCall> createAggregateCalls(Map<Integer, Integer> refMap, int outStartIndex) {
    List<AggregateCall> aggCalls = Lists.newArrayList();
    int outIndex = outStartIndex;
    for (int leftKey = 0; leftKey < joinContext.join.getInput(0).getRowType().getFieldCount(); leftKey++) {
      List<Integer> argList = Lists.newArrayList();
      argList.add(leftKey);
      refMap.put(leftKey, outIndex++);
      aggCalls.add(AggregateCall.create(SqlStdOperatorTable.MIN, false, false,
              argList, -1, joinContext.join.getRowType().getFieldList().get(leftKey).getType(),
              "MIN-SEMI-JOIN" + leftKey));
    }
    return aggCalls;
  }

  @Override
  public boolean go() throws InvalidRelException {
    RelNode top = indexContext.getCall().rel(0);
    if (top instanceof DrillJoinRel) {
      DrillJoinRel join = (DrillJoinRel) top;
      final RelNode input0 = join.getInput(0);
      final RelNode input1 = join.getInput(1);
      RelTraitSet traits0 = input0.getTraitSet().plus(DRILL_PHYSICAL);
      RelNode convertedInput0 = Prule.convert(input0, traits0);
      RelTraitSet traits1 = input1.getTraitSet().plus(DRILL_PHYSICAL);
      RelNode convertedInput1 = Prule.convert(input1, traits1);
      return this.go(top, convertedInput0) && this.go(top, convertedInput1);
    } else {
      return false;
    }
  }
}
