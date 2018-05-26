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
package org.apache.drill.exec.planner.index;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;

public class SemiJoinIndexPlanCallContext {
  public final RelOptRuleCall call;
  public final IndexLogicalPlanCallContext leftSide;
  public final FlattenIndexPlanCallContext rightSide;
  public final DrillJoinRel join;
  public final DrillAggregateRel distinct;
  private boolean isCoveringIndexPlanApplicable = true;


  private IndexLogicalPlanCallContext converingIndexContext;

  public SemiJoinIndexPlanCallContext(RelOptRuleCall call,
                                      DrillJoinRel join,
                                      DrillAggregateRel distinct,
                                      IndexLogicalPlanCallContext leftSide,
                                      FlattenIndexPlanCallContext rightSide) {
    this.call = call;
    this.join = join;
    this.distinct = distinct;
    this.leftSide = leftSide;
    this.rightSide = rightSide;
  }

  public void set(IndexLogicalPlanCallContext coveringIndexContext) {
    this.converingIndexContext = coveringIndexContext;
  }

  public IndexLogicalPlanCallContext getConveringIndexContext() {
    return converingIndexContext;
  }

  public void setCoveringIndexPlanApplicable(boolean isApplicable) {
    this.isCoveringIndexPlanApplicable = isApplicable;
  }

  public boolean isCoveringIndexPlanApplicable() {
    return this.isCoveringIndexPlanApplicable;
  }
}