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
package org.apache.drill.exec.store.mapr.db.json;

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.PathSegment.ArraySegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.hbase.DrillHBaseConstants;
import org.ojai.Value;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryCondition.Op;

import com.mapr.db.impl.MapRDBImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JsonConditionBuilder extends AbstractExprVisitor<JsonScanSpec, Void, RuntimeException> implements DrillHBaseConstants {

  final private JsonTableGroupScan groupScan;

  final private LogicalExpression le;

  private boolean allExpressionsConverted = true;

  // After splitting the array field into prefix and suffix, if the suffix is null use $ as suffix. For ex, if the
  // condition is on a[]. The prefix will be a[] and suffix will be $.
  private static final String defaultField = "$";

  // If the path should be split into prefix and suffix to group same array prefix elements while applying elementAnd.
  // ElementAnd takes array prefix as arg
  private boolean splitArrayPath = false;

  // If the index is part of indexed fields don't use elementAnd. In all other cases elementAnd is used.
  private boolean useElementAnd = true;

  public void setUseElementAnd(boolean useElementAnd) {
    this.useElementAnd = useElementAnd;
  }

  public JsonConditionBuilder(JsonTableGroupScan groupScan,
      LogicalExpression conditionExp) {
    this.groupScan = groupScan;
    this.le = conditionExp;
  }

  public JsonScanSpec parseTree() {
    JsonScanSpec parsedSpec = le.accept(this, null);
    if (parsedSpec != null) {
      parsedSpec.mergeScanSpec("booleanAnd", this.groupScan.getScanSpec());
    }
    return parsedSpec;
  }

  public boolean isAllExpressionsConverted() {
    // TODO Auto-generated method stub
    return allExpressionsConverted;
  }

  @Override
  public JsonScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public JsonScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  private String getEmptyArrayPrefix(SchemaPath schemaPath) {
    String arrayPrefix = getEmptyArrayPath(schemaPath);
    int end = arrayPrefix.lastIndexOf("]") + 1;
    return arrayPrefix.substring(0, end);
  }

  /*
   * Traverse through the path and append "[]" to ArrayFields and return path till the end.
   * For example, If the data is a = [{b:5, c:10}], when referencing a[].b the getEmptyArrayPath
   * returns a[].b where the arrayPrefix is a[] and arraySuffix is b. Incase of a[].b[].c[].d, 
   * the arrayPrefix is a[].b[].c[], arraySuffix is d. Incase of a[].b.c.d, the arrayPrefix is a[] 
   * and arraySuffix is b.c.d
   */
  private String getEmptyArrayPath(SchemaPath schemaPath) {
    String arrayPath = "";
    final String brackets = "[]";
    final String dot = ".";
    if (schemaPath.isArray()) {
      NameSegment nameSegment = schemaPath.getRootSegment();
      while (nameSegment!= null) {
        arrayPath = arrayPath + nameSegment.getPath();
        if (nameSegment.getChild() instanceof ArraySegment && ((ArraySegment) nameSegment.getChild()).getIndex() == -1) {
          arrayPath = arrayPath + brackets;
        }
        if( nameSegment != null ) {
          nameSegment = nameSegment.getChildNameSegment();
        }
        if (nameSegment == null) {
          return arrayPath;
        } else {
          arrayPath = arrayPath + dot;
        }
      }
    }
    return null;
  }

  /*
   * Gets EmptyArrayPrefix if the functioncall has SchemaPath. If the functioncall is "booleanOr" with
   * args, then compares if all the args reference the same EmptyArrayPrefix and gets it else returns null.
   */
  private String getEmptyArrayPrefix(FunctionCall f) {
    String arrayPrefix = null;
    if ("booleanOr".equals(f.getName())){
      arrayPrefix = compareAndGetNestedArgsArrayPrefix(f);
    } else if ( f.args.get(0) instanceof  SchemaPath){
      SchemaPath schemaPath = (SchemaPath) f.args.get(0);
      arrayPrefix = getEmptyArrayPrefix(schemaPath);
    }
    return arrayPrefix;
  }

  /*
   * Gets the last field in the SchemaPath. This is used to get the field under the array element that is being
   * referenced. This is used while grouping all the fields under the same Array element. If the data is a = [{b:5, c:10}],
   * when referencing a[].b, getArraySuffix returns b as suffix.
   */
  private String getArraySuffix(SchemaPath schemaPath) {
    String arrayPath = getEmptyArrayPath(schemaPath);
    // Since suffix starts after "]."
    int suffixStart = arrayPath.lastIndexOf("]") + 2;
    if (suffixStart < arrayPath.length()) {
      return arrayPath.substring(suffixStart);
    }
    return null;
  }

  private String compareAndGetArrayPrefix(FunctionCall exp1, FunctionCall exp2) {
    String s1, s2;
    s1 = getEmptyArrayPrefix(exp1);
    s2 = getEmptyArrayPrefix(exp2);
    if (s1 == null || s2 == null) {
      return null;
    }
    if (s1.equalsIgnoreCase(s2)) {
      return s1;
    }
    return null;
  }

  /*
   * Compares all the args nested under booleanOr to find out if the args belong to same array element and if so return
   * the array prefix path. For example if the condition is a[].b = 10 or a[].b = 20 or a[].b = 30, since all the fields
   * belong to the same array element "a[]", the array prefix path "a[]" will be returned.
   */
  private String compareAndGetNestedArgsArrayPrefix(FunctionCall f) {
    List<LogicalExpression> nestedargs = f.args;
    String arrayPrefix = null;
    if (nestedargs.size() > 1) {
      for (int i = 1; i < nestedargs.size(); i++) {
        arrayPrefix = compareAndGetArrayPrefix((FunctionCall) nestedargs.get(0),(FunctionCall) nestedargs.get(i));
        if ( arrayPrefix == null) {
          return null;
        }
      }
    }
    return arrayPrefix;
  }

  private void addToarrayExprsMap(String path, LogicalExpression f, HashMap<String, List<LogicalExpression>> arrayExprsMap) {
    if (arrayExprsMap.get(path) == null) {
      arrayExprsMap.put(path, new ArrayList<LogicalExpression>());
    }
    arrayExprsMap.get(path).add(f);
  }

  /*
   * Pre-process all the conditions nested under AND. Groups the args that belong to the same array element together.
   * The arrayExprsMap maps array element to the list of all the fields that belong to the array element. If the arg
   * is a boolean operator, all the args nested under booleanOr are compared if they belong to same array element and if
   * so added to the map. All the args in arrayExprsMap and remainder args combined together gives all the args.
   */
  private void preprocessArgs(List<LogicalExpression> args, HashMap<String, List<LogicalExpression>> arrayExprsMap, List<LogicalExpression> remainderArgs) {
    String arrayPrefix;
    for (LogicalExpression f : args ) {
      try {
        if (f instanceof FunctionCall) {
          if ("booleanOr".equals(((FunctionCall) f).getName()) && useElementAnd) {
            arrayPrefix = compareAndGetNestedArgsArrayPrefix((FunctionCall) f);
            if (arrayPrefix != null) {
              addToarrayExprsMap(arrayPrefix, f, arrayExprsMap);
            } else {
              remainderArgs.add(f);
            }
          } else {
            FunctionCall f1 = (FunctionCall) f;
            SchemaPath schemaPath = (SchemaPath) f1.args.get(0);
            if (schemaPath.isArray() && useElementAnd) {
              arrayPrefix = getEmptyArrayPrefix(schemaPath);
              addToarrayExprsMap(arrayPrefix, f, arrayExprsMap);
            } else {
              remainderArgs.add(f);
            }
          }
        } else {
          // For unknown expressions which can't be converted 
	  remainderArgs.add(f);
        }
      }
      catch (Exception e) {
        remainderArgs.add(f);
      }
    }
  }

  @Override
  public JsonScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    JsonScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    List<LogicalExpression> args = call.args;
    List<JsonScanSpec>  conditions = new ArrayList<>();

    if (CompareFunctionsProcessor.isCompareFunction(functionName)) {
      CompareFunctionsProcessor processor;
      if (groupScan.getFormatPlugin().getConfig().isReadTimestampWithZoneOffset()) {
        processor = CompareFunctionsProcessor.processWithTimeZoneOffset(call);
      } else {
        processor = CompareFunctionsProcessor.process(call);
      }
      if (processor.isSuccess()) {
        nodeScanSpec = createJsonScanSpec(call, processor);
      }
    } else {
      switch(functionName) {
      case "booleanAnd":
        /*
         * Holds the array element path and the list of logical expressions that belong to the array element. This grouping
         * is needed for elementAnd. For example if condition is a[].b = 1 and a[].c = 2 and d = 10. The first two expressions
         * are grouped under "a[]".
         */
        HashMap<String, List<LogicalExpression>> arrayExprsMap = new HashMap<>();
        List<LogicalExpression> remainderArgs = new ArrayList<>();
        preprocessArgs(args, arrayExprsMap, remainderArgs);
        HashMap<String, List<LogicalExpression>> arrayPrefixArgs = arrayExprsMap;
        List<LogicalExpression> scalarArgs = remainderArgs;
        JsonScanSpec nextScanSpec = null;
        JsonScanSpec nodeScanSpec1 = null;

        for (String arrayPrefix : arrayPrefixArgs.keySet()) {
          List<LogicalExpression> elementAndArgs = arrayPrefixArgs.get(arrayPrefix);
          // If there is only one Arg that belongs to that array element, treat it as regular 'AND'
          if (elementAndArgs.size() == 1) {
            scalarArgs.addAll(elementAndArgs);
          } else {
            splitArrayPath = true;
            conditions.clear();
            nextScanSpec = null;
            nodeScanSpec = elementAndArgs.get(0).accept(this, null);

            for (int i = 1; i < elementAndArgs.size(); i++) {
              nextScanSpec = elementAndArgs.get(i).accept(this, null);
              if (nodeScanSpec != null && nextScanSpec != null) {
                conditions.add(nextScanSpec);
              } else {
                allExpressionsConverted = false;
                nodeScanSpec = nodeScanSpec == null ? nextScanSpec : nodeScanSpec;
              }
            }
            nodeScanSpec.mergeScanSpec("elementAnd", conditions, arrayPrefix);
            splitArrayPath = false;
          }
        }

        if (scalarArgs.size() > 0) {
          nodeScanSpec1 = scalarArgs.get(0).accept(this, null);
          if (nodeScanSpec1 == null) {
            allExpressionsConverted = false;
          }
        }

        for (int i = 1; i < scalarArgs.size(); i++ ) {
          nextScanSpec = scalarArgs.get(i).accept(this, null);
          if (nodeScanSpec1 != null && nextScanSpec != null) {
            nodeScanSpec1.mergeScanSpec(functionName, nextScanSpec);
          } else {
            allExpressionsConverted = false;
            nodeScanSpec1 = nodeScanSpec1 == null ? nextScanSpec : nodeScanSpec1;
          }
        }
        if (nodeScanSpec != null && nodeScanSpec1 != null) {
          nodeScanSpec.mergeScanSpec(functionName, nodeScanSpec1);
        } else {
          nodeScanSpec = nodeScanSpec == null ? nodeScanSpec1 : nodeScanSpec;
        }

        break;
      case "booleanOr":
        nodeScanSpec = args.get(0).accept(this, null);
        for (int i = 1; i < args.size(); ++i) {
          nextScanSpec = args.get(i).accept(this, null);
          if (nodeScanSpec != null && nextScanSpec != null) {
              nodeScanSpec.mergeScanSpec(functionName, nextScanSpec);
          } else {
            allExpressionsConverted = false;
          }
        }
        break;

      case "ojai_sizeof":
      case "ojai_typeof":
      case "ojai_nottypeof":
      case "ojai_matches":
      case "ojai_notmatches":
      case "ojai_condition": {
        final OjaiFunctionsProcessor processor = OjaiFunctionsProcessor.process(call);
        if (processor != null) {
                return new JsonScanSpec(groupScan.getTableName(), groupScan.getIndexDesc(),
                                processor.getCondition());
        }
      }
      }
    }

    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }

    return nodeScanSpec;
  }

  private void setIsCondition(QueryCondition c,
                              String str,
                              QueryCondition.Op op,
                              Value v) {
    switch (v.getType()) {
    case BOOLEAN:
      c.is(str, op, v.getBoolean());
      break;
    case STRING:
      c.is(str, op, v.getString());
      break;
    case BYTE:
      c.is(str, op, v.getByte());
      break;
    case SHORT:
      c.is(str, op, v.getShort());
      break;
    case INT:
      c.is(str, op, v.getInt());
      break;
    case LONG:
      c.is(str, op, v.getLong());
      break;
    case FLOAT:
      c.is(str, op, v.getFloat());
      break;
    case DOUBLE:
      c.is(str, op, v.getDouble());
      break;
    case DECIMAL:
      c.is(str, op, v.getDecimal());
      break;
    case DATE:
      c.is(str, op, v.getDate());
      break;
    case TIME:
      c.is(str, op, v.getTime());
      break;
    case TIMESTAMP:
      c.is(str, op, v.getTimestamp());
      break;
    case BINARY:
      c.is(str, op, v.getBinary());
      break;
      case ARRAY:
        c.equals(str, v.getList());
        break;
      case MAP:
        c.equals(str, v.getMap());
        break;
    default:
      break;
    }
  }

  private JsonScanSpec createJsonScanSpec(FunctionCall call,
      CompareFunctionsProcessor processor) {
    String functionName = processor.getFunctionName();
    String fieldPath = FieldPathHelper.schemaPath2FieldPath(processor.getPath()).asPathString();
    Value fieldValue = processor.getValue();
    SchemaPath schemaPath = processor.getPath();

    if (schemaPath.isArray()) {
      String arrayPrefix = getEmptyArrayPrefix(schemaPath);
      if (splitArrayPath) {
        fieldPath = getArraySuffix(schemaPath);
        fieldPath = fieldPath == null ? defaultField : fieldPath;
      }
    }

    QueryCondition cond = null;
    switch (functionName) {
    case "equal":
      cond = MapRDBImpl.newCondition();
      setIsCondition(cond, fieldPath, Op.EQUAL, fieldValue);
      break;

    case "not_equal":
      cond = MapRDBImpl.newCondition();
      setIsCondition(cond, fieldPath, Op.NOT_EQUAL, fieldValue);
      break;

    case "less_than":
      cond = MapRDBImpl.newCondition();
      setIsCondition(cond, fieldPath, Op.LESS, fieldValue);
      break;

    case "less_than_or_equal_to":
      cond = MapRDBImpl.newCondition();
      setIsCondition(cond, fieldPath, Op.LESS_OR_EQUAL, fieldValue);
      break;

    case "greater_than":
      cond = MapRDBImpl.newCondition();
      setIsCondition(cond, fieldPath, Op.GREATER, fieldValue);
      break;

    case "greater_than_or_equal_to":
      cond = MapRDBImpl.newCondition();
      setIsCondition(cond, fieldPath, Op.GREATER_OR_EQUAL, fieldValue);
      break;

    case "isnull":
      // 'field is null' should be transformed to 'field not exists OR typeof(field) = NULL'
      QueryCondition orCond = MapRDBImpl.newCondition().or();
      cond = orCond.notExists(fieldPath).typeOf(fieldPath, Value.Type.NULL).close();
      break;

    case "isnotnull":
      // 'field is not null should be transformed to 'field exists AND typeof(field) != NULL'
      QueryCondition andCond = MapRDBImpl.newCondition().and();
      cond = andCond.exists(fieldPath).notTypeOf(fieldPath, Value.Type.NULL).close();
      break;

    case "istrue":
      cond = MapRDBImpl.newCondition().is(fieldPath, Op.EQUAL, true);
      break;

    case "isnotfalse":
      cond = MapRDBImpl.newCondition().is(fieldPath, Op.NOT_EQUAL, false);
      break;

    case "isfalse":
      cond = MapRDBImpl.newCondition().is(fieldPath, Op.EQUAL, false);
      break;

    case "isnottrue":
      cond = MapRDBImpl.newCondition().is(fieldPath, Op.NOT_EQUAL, true);
      break;

    case "like":
      cond = MapRDBImpl.newCondition().like(fieldPath, fieldValue.getString());
      break;

    default:
    }

    if (cond != null) {
      return new JsonScanSpec(groupScan.getTableName(),
                              groupScan.getIndexDesc(),
                              cond.build());
    }

    return null;
  }
}
