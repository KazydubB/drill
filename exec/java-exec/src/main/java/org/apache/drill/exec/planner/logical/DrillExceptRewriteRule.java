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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Deprecated
public class DrillExceptRewriteRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new DrillExceptRewriteRule();

  private DrillExceptRewriteRule() {
//    super(RelOptHelper.any(DrillExceptRel.class, Convention.NONE), // todo: or Minus instead?
//    super(RelOptHelper.any(DrillExceptRel.class, DrillRel.DRILL_LOGICAL), // todo: or Minus instead? LOGICAL?
//    super(RelOptHelper.any(DrillExceptRel.class, DrillRel.DRILL_LOGICAL), // todo: or Minus instead? LOGICAL?
    super(RelOptHelper.any(LogicalMinus.class), // todo: or Minus instead? LOGICAL?
//    super(RelOptHelper.any(Minus.class), // todo: or Minus instead? LOGICAL?
        DrillRelFactories.LOGICAL_BUILDER, "DrillExceptRewriteRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    System.out.println("Hello from rewrite rule!");
//    DrillExceptRel except = call.rel(0);
    LogicalMinus except = call.rel(0);

    RelOptCluster cluster = except.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();
//    ImmutableList.Builder<RelNode> builder = new ImmutableList.Builder<>();
    List<RelNode> nodes = new ArrayList<>(2);

//    try {
      nodes.add(createFirstGB(convert(except.getInputs().get(0), except.getInputs().get(0).getTraitSet().plus(DrillRel.DRILL_LOGICAL)), true, cluster, rexBuilder));
//      builder.add(createFirstGB(convert(except.getInputs().get(0), except.getInputs().get(0).getTraitSet().plus(DrillRel.DRILL_LOGICAL)), true, cluster, rexBuilder));
      nodes.add(createFirstGB(convert(except.getInputs().get(1), except.getInputs().get(1).getTraitSet().plus(DrillRel.DRILL_LOGICAL)), false, cluster, rexBuilder));
//      builder.add(createFirstGB(convert(except.getInputs().get(1), except.getInputs().get(1).getTraitSet().plus(DrillRel.DRILL_LOGICAL)), false, cluster, rexBuilder));
    /*} catch (CalciteSemanticException e) {
      LOG.debug(e.toString());
      throw new RuntimeException(e);
    }*/

    // create a union above all the branches
    // the schema of union looks like this
    // all keys + VCol + c
    // HiveRelNode union = new HiveUnion(cluster, TraitsUtil.getDefaultTraitSet(cluster), bldr.build());
    RelNode union = null; // todo: attention to the arguments
    try {
      union = new DrillUnionRel(cluster, except.getTraitSet().plus(DrillRel.DRILL_LOGICAL), nodes, true, true); // todo: traits!
    } catch (InvalidRelException e) {
      // e.printStackTrace();
      throw new DrillRuntimeException(); // todo: no
    }

    // 2nd level GB: create a GB (all keys + sum(c) as a + sum(VCol*c) as b) for
    // each branch
    final List<RexNode> gbChildProjLst = new ArrayList<>();
    final List<Integer> groupSetPositions = new ArrayList<>();
    int unionColumnSize = union.getRowType().getFieldList().size();
    for (int cInd = 0; cInd < unionColumnSize; cInd++) {
      gbChildProjLst.add(rexBuilder.makeInputRef(union, cInd));
      // the last 2 columns are VCol and c
      if (cInd < unionColumnSize - 2) {
        groupSetPositions.add(cInd);
      }
    }

//    try {
      gbChildProjLst.add(multiply(rexBuilder.makeInputRef(union, unionColumnSize - 2),
          rexBuilder.makeInputRef(union, unionColumnSize - 1), cluster, rexBuilder));
      union.getInput(unionColumnSize - 2);
//    } catch (CalciteSemanticException e) {
//       LOG.debug(e.toString());
//      throw new RuntimeException(e);
//    }

    RelNode gbInputRel;
//    try {
      // Here we create a project for the following reasons:
      // (1) GBy only accepts arg as a position of the input, however, we need to sum on VCol*c
      // (2) This can better reuse the function createSingleArgAggCall.
      // gbInputRel = HiveProject.create(union, gbChildProjLst, null);
//    RelTraitSet traits = RelTraitSet.createEmpty(); // todo: this should be changed: with replace(...); see some examples
//    RelTraitSet traits = cluster.traitSetOf(Convention.NONE); // todo: this should be changed: with replace(...); see some examples
    RelDataType rowType = RexUtil.createStructType(
        cluster.getTypeFactory(), gbChildProjLst, null, SqlValidatorUtil.EXPR_SUGGESTER);
      gbInputRel = DrillProjectRel.create(cluster, union.getTraitSet().plus(DrillRel.DRILL_LOGICAL), union,
          gbChildProjLst, rowType); // todo: traits!
//    } catch (CalciteSemanticException e) {
      // LOG.debug(e.toString());
//      throw new RuntimeException(e);
//    }

    // gbInputRel's schema is like this
    // all keys + VCol + c + VCol*c
    List<AggregateCall> aggregateCalls = new ArrayList<>();
    RelDataType aggFnRetType = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

    // sum(c)
//    AggregateCall aggregateCall = HiveCalciteUtil.createSingleArgAggCall("sum", cluster,
//        TypeInfoFactory.longTypeInfo, unionColumnSize - 1, aggFnRetType);
    List<Integer> pos = Collections.singletonList(unionColumnSize - 1);
    AggregateCall aggregateCall = AggregateCall.create(DrillOperatorTable.SUM, false, pos, -1, aggFnRetType, null);
    aggregateCalls.add(aggregateCall);

    // sum(VCol*c)
//    aggregateCall = HiveCalciteUtil.createSingleArgAggCall("sum", cluster,
//        TypeInfoFactory.longTypeInfo, unionColumnSize, aggFnRetType);
    pos = Collections.singletonList(unionColumnSize);
    aggregateCall = AggregateCall.create(DrillOperatorTable.SUM, false, pos, -1, aggFnRetType, null);
    aggregateCalls.add(aggregateCall);

    final ImmutableBitSet groupSet = ImmutableBitSet.of(groupSetPositions);
//    RelNode aggregateRel = new HiveAggregate(cluster,
//        cluster.traitSetOf(HiveRelNode.CONVENTION), gbInputRel, groupSet, null,
//    RelNode aggregateRel = new DrillAggregateRel(cluster, cluster.traitSetOf(Convention.NONE), gbInputRel, groupSet, null, // todo: traits!
    RelNode aggregateRel = new DrillAggregateRel(cluster, gbInputRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL), gbInputRel, groupSet, null, // todo: traits!
        aggregateCalls);

    // the schema after GB is like this
    // all keys + sum(c) as a + sum(VCol*c) as b
    // the column size is the same as unionColumnSize;
    // (1) for except distinct add a filter (b-a>0 && 2a-b=0)
    // i.e., a > 0 && 2a = b
    // then add the project
    // (2) for except all add a project to change it to
    // (2b-3a) + all keys
    // then add the UDTF

    if (!except.all) {
      RelNode filterRel = null;
//      try {
        /*filterRel = new HiveFilter(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
            aggregateRel, makeFilterExprForExceptDistinct(aggregateRel, unionColumnSize, cluster,
            rexBuilder));*/
//        filterRel = new DrillFilterRel(cluster, cluster.traitSetOf(Convention.NONE), aggregateRel, makeFilterExprForExceptDistinct(aggregateRel, unionColumnSize, cluster,
        filterRel = new DrillFilterRel(cluster, except.getTraitSet().plus(DrillRel.DRILL_LOGICAL), aggregateRel, makeFilterExprForExceptDistinct(aggregateRel, unionColumnSize, cluster,
            rexBuilder)); // todo: traits!
//      } catch (CalciteSemanticException e) {
        // LOG.debug(e.toString());
//        throw new RuntimeException(e);
//      }

      // finally add a project to project out the last 2 columns
      Set<Integer> projectOutColumnPositions = new HashSet<>();
      projectOutColumnPositions.add(filterRel.getRowType().getFieldList().size() - 2);
      projectOutColumnPositions.add(filterRel.getRowType().getFieldList().size() - 1);
//      try {
//        call.transformTo(HiveCalciteUtil.createProjectWithoutColumn(filterRel,

      call.getPlanner().setImportance(except, 0);

        call.transformTo(createProjectWithoutColumn(filterRel,
            projectOutColumnPositions));
//      } catch (CalciteSemanticException e) {
        // LOG.debug(e.toString());
//        throw new RuntimeException(e);
//      }
    } else {
      List<RexNode> originalInputRefs = aggregateRel.getRowType().getFieldList().stream()
          .map(input -> new RexInputRef(input.getIndex(), input.getType()))
          .collect(Collectors.toList());

      List<RexNode> copyInputRefs = new ArrayList<>();
//      try {
        copyInputRefs.add(makeExprForExceptAll(aggregateRel, unionColumnSize, cluster, rexBuilder));
//      } catch (CalciteSemanticException e) {
        // LOG.debug(e.toString());
//        throw new RuntimeException(e);
//      }
      for (int i = 0; i < originalInputRefs.size() - 2; i++) {
        copyInputRefs.add(originalInputRefs.get(i));
      }
      /*RelNode srcRel = null;
//      try {
        srcRel = HiveProject.create(aggregateRel, copyInputRefs, null);
        HiveTableFunctionScan udtf = HiveCalciteUtil.createUDTFForSetOp(cluster, srcRel);
        // finally add a project to project out the 1st columns
        Set<Integer> projectOutColumnPositions = new HashSet<>();
        projectOutColumnPositions.add(0);
        call.transformTo(HiveCalciteUtil
            .createProjectWithoutColumn(udtf, projectOutColumnPositions));*/ // todo: this should be resolved somehow
//      } catch (SemanticException e) {
//        LOG.debug(e.toString());
//        throw new RuntimeException(e);
//      }
    }
  }

  public static DrillProjectRel createProjectWithoutColumn(RelNode input, Set<Integer> positions) {
    System.out.println("Create project without columns");
    List<RexNode> originalInputRefs = input.getRowType().getFieldList().stream()
        .map(input1 -> new RexInputRef(input1.getIndex(), input1.getType()))
        .collect(Collectors.toList());
    List<RexNode> copyInputRefs = new ArrayList<>();
    for (int i = 0; i < originalInputRefs.size(); i++) {
      if (!positions.contains(i)) {
        copyInputRefs.add(originalInputRefs.get(i));
      }
    }
//    return DrillProjectRel.create(input.getCluster(), input.getCluster().traitSetOf(Convention.NONE), input, copyInputRefs, null);
    RelDataType rowType = RexUtil.createStructType(
        input.getCluster().getTypeFactory(), copyInputRefs, null, SqlValidatorUtil.EXPR_SUGGESTER);
    return DrillProjectRel.create(input.getCluster(), input.getTraitSet().plus(DrillRel.DRILL_LOGICAL), input, copyInputRefs, rowType); // todo: traits!
  }

  private RelNode createFirstGB(RelNode input, boolean left, RelOptCluster cluster,
                                RexBuilder rexBuilder) {// throws CalciteSemanticException {
    final List<RexNode> gbChildProjLst = new ArrayList<>();
    final List<Integer> groupSetPositions = new ArrayList<>();
    for (int cInd = 0; cInd < input.getRowType().getFieldList().size(); cInd++) {
      gbChildProjLst.add(rexBuilder.makeInputRef(input, cInd));
      groupSetPositions.add(cInd);
    }
    if (left) {
      gbChildProjLst.add(rexBuilder.makeBigintLiteral(BigDecimal.valueOf(2)));
    } else {
      gbChildProjLst.add(rexBuilder.makeBigintLiteral(BigDecimal.ONE));
    }

    // also add the last VCol
    groupSetPositions.add(input.getRowType().getFieldList().size());

    // create the project before GB
    RelDataType rowType = RexUtil.createStructType(
        cluster.getTypeFactory(), gbChildProjLst, null, SqlValidatorUtil.EXPR_SUGGESTER);
    RelNode gbInputRel = DrillProjectRel.create(cluster, input.getTraitSet().plus(DrillRel.DRILL_LOGICAL), input, gbChildProjLst, rowType); // todo: arguments, esp. traits

    // groupSetPosition includes all the positions
    final ImmutableBitSet groupSet = ImmutableBitSet.of(groupSetPositions);

    List<AggregateCall> aggregateCalls = new ArrayList<>();
    RelDataType aggFnRetType = cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

//    AggregateCall aggregateCall = HiveCalciteUtil.createSingleArgAggCall("count", cluster,
//        TypeInfoFactory.longTypeInfo, input.getRowType().getFieldList().size(), aggFnRetType);

    List<Integer> pos = Collections.singletonList(input.getRowType().getFieldCount());
    AggregateCall aggregateCall = AggregateCall.create(DrillOperatorTable.COUNT, false, pos, -1, aggFnRetType, null);

    /*AggregateCall aggregateCall = AggregateCall.create(new SqlSumAggFunction(aggFnRetType), false, Collections.singletonList(input.getRowType().getFieldCount()),
        -1, cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), "name"); // todo:*/
    // create normally
    aggregateCalls.add(aggregateCall);

//    return new DrillAggregateRel(cluster, cluster.traitSetOf(Convention.NONE), gbInputRel, groupSet, null, aggregateCalls); // todo: TraitSet?
    return new DrillAggregateRel(cluster, gbInputRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL), gbInputRel, groupSet, null, aggregateCalls); // todo: TraitSet?

//    return new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), gbInputRel,
//        groupSet, null, aggregateCalls);
  }

   private RexNode multiply(RexNode r1, RexNode r2, RelOptCluster cluster, RexBuilder rexBuilder) {
//  private RexNode multiply(RelNode r1, RelNode r2, RelOptCluster cluster, RexBuilder rexBuilder)
      // throws CalciteSemanticException {
//    List<RexNode> childRexNodeLst = new ArrayList<>();
//    childRexNodeLst.add(r1);
//    childRexNodeLst.add(r2);
//    RelNode[] nodes = new RelNode[2];
//    nodes[0] = r1;
//    nodes[1] = r2;
//    ImmutableList.Builder<RelDataType> calciteArgTypesBldr = new ImmutableList.Builder<>();
    //List<RelDataType> dataTypes = new ArrayList<>(2);
//    calciteArgTypesBldr.add(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
    //dataTypes.add(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
//    calciteArgTypesBldr.add(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
    //dataTypes.add(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT));

    return cluster.getRexBuilder().makeCall(DrillOperatorTable.MULTIPLY, r1, r2); // todo 1st arg may be different

//    DrillOperatorTable.instance().lookupOperatorOverloads();

    // SqlStdOperatorTable.MULTIPLY.createCall(SqlParserPos.ZERO, r1, r2);

//    return rexBuilder.makeCall(
//        SqlFunctionConverter.getCalciteFn("*", calciteArgTypesBldr.build(),
//            cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), true, false),
//        childRexNodeLst);
  }

  private RexNode makeFilterExprForExceptDistinct(RelNode input, int columnSize,
                                                  RelOptCluster cluster, RexBuilder rexBuilder) {//throws CalciteSemanticException {
    List<RexNode> childRexNodeLst = new ArrayList<>();
    RexInputRef a = rexBuilder.makeInputRef(input, columnSize - 2);
    RexLiteral zero = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(0));
    childRexNodeLst.add(a);
    childRexNodeLst.add(zero);
//    ImmutableList.Builder<RelDataType> calciteArgTypesBldr = new ImmutableList.Builder<RelDataType>();
    //List<RelDataType> dataTypes = new ArrayList<>(2);
//    calciteArgTypesBldr.add(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
    //dataTypes.add(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
//    calciteArgTypesBldr.add(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
    //dataTypes.add(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
    // a>0
    RexNode aMorethanZero = rexBuilder.makeCall(
//        SqlFunctionConverter.getCalciteFn(">", calciteArgTypesBldr.build(),
//            cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), false, false),
        DrillOperatorTable.GREATER_THAN, // todo: OK?
        childRexNodeLst);
    childRexNodeLst = new ArrayList<>();
    RexLiteral two = rexBuilder.makeBigintLiteral(new BigDecimal(2));
    childRexNodeLst.add(a);
    childRexNodeLst.add(two);
    // 2*a
    RexNode twoa = rexBuilder.makeCall(
//        SqlFunctionConverter.getCalciteFn("*", calciteArgTypesBldr.build(),
//            cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), false, false),
        DrillOperatorTable.MULTIPLY, // todo: is OK?
        childRexNodeLst);
    childRexNodeLst = new ArrayList<>();
    RexInputRef b = rexBuilder.makeInputRef(input, columnSize - 1);
    childRexNodeLst.add(twoa);
    childRexNodeLst.add(b);
    // 2a=b
    RexNode twoaEqualTob = rexBuilder.makeCall(
//        SqlFunctionConverter.getCalciteFn("=", calciteArgTypesBldr.build(),
//            cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), false, false),
        DrillOperatorTable.EQUALS, // todo: is OK?
        childRexNodeLst);
    childRexNodeLst = new ArrayList<>();
    childRexNodeLst.add(aMorethanZero);
    childRexNodeLst.add(twoaEqualTob);
    // a>0 && 2a=b
    return rexBuilder.makeCall(
//        SqlFunctionConverter.getCalciteFn("and", calciteArgTypesBldr.build(),
//            cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), false, false),
        DrillOperatorTable.AND, // todo: is OK?
        childRexNodeLst);
  }

  private RexNode makeExprForExceptAll(RelNode input, int columnSize, RelOptCluster cluster,
                                       RexBuilder rexBuilder) {//throws CalciteSemanticException {
    List<RexNode> childRexNodeLst = new ArrayList<>();
//    ImmutableList.Builder<RelDataType> calciteArgTypesBldr = new ImmutableList.Builder<>();
//    calciteArgTypesBldr.add(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
//    calciteArgTypesBldr.add(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
    RexInputRef a = rexBuilder.makeInputRef(input, columnSize - 2);
    RexLiteral three = rexBuilder.makeBigintLiteral(new BigDecimal(3));
    childRexNodeLst.add(three);
    childRexNodeLst.add(a);
    RexNode threea = rexBuilder.makeCall(
//        SqlFunctionConverter.getCalciteFn("*", calciteArgTypesBldr.build(),
//            cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), false, false),
        DrillOperatorTable.MULTIPLY, // todo: is OK?
        childRexNodeLst);

    RexLiteral two = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(2));
    RexInputRef b = rexBuilder.makeInputRef(input, columnSize - 1);

    // 2*b
    childRexNodeLst = new ArrayList<>();
    childRexNodeLst.add(two);
    childRexNodeLst.add(b);
    RexNode twob = rexBuilder.makeCall(
//        SqlFunctionConverter.getCalciteFn("*", calciteArgTypesBldr.build(),
//            cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), false, false),
        DrillOperatorTable.MULTIPLY, // todo: is OK?
        childRexNodeLst);

    // 2b-3a
    childRexNodeLst = new ArrayList<>();
    childRexNodeLst.add(twob);
    childRexNodeLst.add(threea);
    return rexBuilder.makeCall(
//        SqlFunctionConverter.getCalciteFn("-", calciteArgTypesBldr.build(),
//            cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), false, false),
        DrillOperatorTable.MINUS, // todo: is OK?
        childRexNodeLst);
  }
}
