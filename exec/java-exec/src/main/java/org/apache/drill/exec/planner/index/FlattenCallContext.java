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
package org.apache.drill.exec.planner.index;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.common.DrillFilterRelBase;
import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.common.DrillScanRelBase;

public interface FlattenCallContext {

  LinkedHashMap<RelNode, Map<String, RexCall>> getProjectToFlattenMapForAllProjects();

  Map<String, RexCall> getFlattenMapForProject(RelNode project);

  LinkedHashMap<RelNode, List<RexNode>> getNonFlattenExprsMapForAllProjects();

  List<RexNode> getNonFlattenExprsForProject(RelNode project);

  DrillFilterRelBase getFilterAboveRootFlatten();

  DrillFilterRelBase getFilterBelowLeafFlatten();

  void setFilterExprsReferencingFlatten(Map<String, List<RexNode>> exprsReferencingFlattenMap);

  DrillProjectRelBase getProjectWithRootFlatten();

  Map<String, List<RexNode>> getFilterExprsReferencingFlatten();

  DrillProjectRelBase getLeafProjectAboveScan();

  void setRelevantExprsInLeafProject(List<RexNode> exprList);

  List<RexNode> getRelevantExprsInLeafProject();

  void setExprsForLeafFilter(List<RexInputRef> exprList);

  List<RexInputRef> getExprsForLeafFilter();

  DrillScanRelBase getScan();

  DrillProjectRelBase getProjectAboveRootFlatten();

  void setFilterBelowLeafFlatten(DrillFilterRelBase filter);

  void setLeafProjectAboveScan(DrillProjectRelBase project);

}
