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
package org.apache.drill.common.expression.fn;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;

public class FunctionsUtil {

  private static Map<MinorType, String> TYPE_TO_CAST_FUNC = new HashMap<>();
  // Maps function to supported input types for substitution
  private static Map<String, Set<MinorType>> FUNC_TO_INPUT_TYPES = new HashMap<>();
  /** The functions that need to be replaced (if
   * "drill.exec.functions.cast_empty_string_to_null" is set to true). */
  private static Set<String> FUNC_REPLACEMENT_NEEDED = new HashSet<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARCHAR). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NON_NULLABLE_VARCHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for non-nullable VAR16CHAR). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NON_NULLABLE_VAR16CHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARBINARY). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NON_NULLABLE_VARBINARY = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARCHAR). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VAR16CHAR). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARBINARY). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY = new HashMap<>();

  static {
    initCastFunctionSubstitutions();
    initToFunctionSubstitutions();
  }

  private static void initCastFunctionSubstitutions() {
    TYPE_TO_CAST_FUNC.put(MinorType.UNION, "castUNION");
    TYPE_TO_CAST_FUNC.put(MinorType.BIGINT, "castBIGINT");
    TYPE_TO_CAST_FUNC.put(MinorType.INT, "castINT");
    TYPE_TO_CAST_FUNC.put(MinorType.BIT, "castBIT");
    TYPE_TO_CAST_FUNC.put(MinorType.TINYINT, "castTINYINT");
    TYPE_TO_CAST_FUNC.put(MinorType.FLOAT4, "castFLOAT4");
    TYPE_TO_CAST_FUNC.put(MinorType.FLOAT8, "castFLOAT8");
    TYPE_TO_CAST_FUNC.put(MinorType.VARCHAR, "castVARCHAR");
    TYPE_TO_CAST_FUNC.put(MinorType.VAR16CHAR, "castVAR16CHAR");
    TYPE_TO_CAST_FUNC.put(MinorType.VARBINARY, "castVARBINARY");
    TYPE_TO_CAST_FUNC.put(MinorType.DATE, "castDATE");
    TYPE_TO_CAST_FUNC.put(MinorType.TIME, "castTIME");
    TYPE_TO_CAST_FUNC.put(MinorType.TIMESTAMP, "castTIMESTAMP");
    TYPE_TO_CAST_FUNC.put(MinorType.TIMESTAMPTZ, "castTIMESTAMPTZ");
    TYPE_TO_CAST_FUNC.put(MinorType.INTERVALDAY, "castINTERVALDAY");
    TYPE_TO_CAST_FUNC.put(MinorType.INTERVALYEAR, "castINTERVALYEAR");
    TYPE_TO_CAST_FUNC.put(MinorType.INTERVAL, "castINTERVAL");
    TYPE_TO_CAST_FUNC.put(MinorType.DECIMAL9, "castDECIMAL9");
    TYPE_TO_CAST_FUNC.put(MinorType.DECIMAL18, "castDECIMAL18");
    TYPE_TO_CAST_FUNC.put(MinorType.DECIMAL28SPARSE, "castDECIMAL28SPARSE");
    TYPE_TO_CAST_FUNC.put(MinorType.DECIMAL28DENSE, "castDECIMAL28DENSE");
    TYPE_TO_CAST_FUNC.put(MinorType.DECIMAL38SPARSE, "castDECIMAL38SPARSE");
    TYPE_TO_CAST_FUNC.put(MinorType.DECIMAL38DENSE, "castDECIMAL38DENSE");
    TYPE_TO_CAST_FUNC.put(MinorType.VARDECIMAL, "castVARDECIMAL");

    // Numeric types
    setupReplacementFunctionsForCast(MinorType.INT, "NullableINT");
    setupReplacementFunctionsForCast(MinorType.BIGINT, "NullableBIGINT");
    setupReplacementFunctionsForCast(MinorType.FLOAT4, "NullableFLOAT4");
    setupReplacementFunctionsForCast(MinorType.FLOAT8, "NullableFLOAT8");
    setupReplacementFunctionsForCast(MinorType.DECIMAL9, "NullableDECIMAL9");
    setupReplacementFunctionsForCast(MinorType.DECIMAL18, "NullableDECIMAL18");
    setupReplacementFunctionsForCast(MinorType.DECIMAL28SPARSE, "NullableDECIMAL28SPARSE");
    setupReplacementFunctionsForCast(MinorType.DECIMAL38SPARSE, "NullableDECIMAL38SPARSE");
    setupReplacementFunctionsForCast(MinorType.VARDECIMAL, "NullableVARDECIMAL");
    // date/time types
    setupReplacementFunctionsForCast(MinorType.DATE, "NULLABLEDATE");
    setupReplacementFunctionsForCast(MinorType.TIME, "NULLABLETIME");
    setupReplacementFunctionsForCast(MinorType.TIMESTAMP, "NULLABLETIMESTAMP");
    // interval types
    setupReplacementFunctionsForCast(MinorType.INTERVAL, "NullableINTERVAL");
    setupReplacementFunctionsForCast(MinorType.INTERVALDAY, "NullableINTERVALDAY");
    setupReplacementFunctionsForCast(MinorType.INTERVALYEAR, "NullableINTERVALYEAR");
  }

  private static void initToFunctionSubstitutions() {
    setupReplacementFunctionsForTo("to_date", "ToNullableDate");
    setupReplacementFunctionsForTo("to_time", "ToNullableTime");
    setupReplacementFunctionsForTo("to_timestamp", "ToNullableTimeStamp");

    setupReplacementFunctionsForTo("sql_to_date", "SqlToNullableDate");
    setupReplacementFunctionsForTo("sql_to_time", "SqlToNullableTime");
    setupReplacementFunctionsForTo("sql_to_timestamp", "SqlToNullableTimeStamp");
  }

  private static void setupReplacementFunctionsForCast(MinorType type, String toType) {
    String functionName = TYPE_TO_CAST_FUNC.get(type);

    FUNC_REPLACEMENT_NEEDED.add(functionName);
    Set<MinorType> supportedInputTypes = new HashSet<>(
        Arrays.asList(MinorType.VARCHAR, MinorType.VAR16CHAR, MinorType.VARBINARY));
    FUNC_TO_INPUT_TYPES.put(functionName, supportedInputTypes);

    FUNC_REPLACEMENT_FROM_NON_NULLABLE_VARCHAR.put(functionName, "castEmptyStringVarCharTo" + toType);
    FUNC_REPLACEMENT_FROM_NON_NULLABLE_VAR16CHAR.put(functionName, "castEmptyStringVar16CharTo" + toType);
    FUNC_REPLACEMENT_FROM_NON_NULLABLE_VARBINARY.put(functionName, "castEmptyStringVarBinaryTo" + toType);

    FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(functionName, "castEmptyStringNullableVarCharTo" + toType);
    FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(functionName, "castEmptyStringNullableVar16CharTo" + toType);
    FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(functionName, "castEmptyStringNullableVarBinaryTo" + toType);
  }

  private static void setupReplacementFunctionsForTo(String functionName, String toSuffix) {
    Set<MinorType> typeSet = Collections.singleton(MinorType.VARCHAR);
    FUNC_TO_INPUT_TYPES.put(functionName, typeSet);
    FUNC_REPLACEMENT_NEEDED.add(functionName);

    FUNC_REPLACEMENT_FROM_NON_NULLABLE_VARCHAR.put(functionName,"convertVarChar" + toSuffix);
    FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(functionName, "convertNullableVarChar" + toSuffix);
  }

  /**
  * Given the target type, get the appropriate cast function
  * @param targetMinorType the target data type
  * @return the name of cast function
  */
  public static String getCastFunc(MinorType targetMinorType) {
    String func = TYPE_TO_CAST_FUNC.get(targetMinorType);
    if (func != null) {
      return func;
    }

    throw new IllegalArgumentException(
      String.format("cast function for type %s is not defined", targetMinorType.name()));
  }

  /**
  * Get a replacing cast function for the original function, based on the specified data mode
  * @param functionName original cast function
  * @param dataMode data mode of the input data
  * @param inputType input (minor) type for cast
  * @return the name of replaced cast function
  */
  public static String getReplacingFunction(String functionName, DataMode dataMode, MinorType inputType) {
    if (dataMode == DataMode.OPTIONAL) {
      return getReplacingFunctionFromNullable(functionName, inputType);
    }

    if (dataMode == DataMode.REQUIRED) {
      return getReplacingFunctionFromNonNullable(functionName, inputType);
    }

    throw new DrillRuntimeException(
       String.format("replacing cast function for datatype %s is not defined", dataMode));
  }

  /**
  * Check if a replacing cast function is available for the the original function
  * @param originalfunction original cast function
  * @param inputType input (minor) type for cast
  * @return true if replacement is needed, false - if isn't
  */
  public static boolean isReplacementNeeded(String originalfunction, MinorType inputType) {
    return FUNC_REPLACEMENT_NEEDED.contains(originalfunction)
        && FUNC_TO_INPUT_TYPES.get(originalfunction).contains(inputType);
  }

  /**
   * Check if a funcName is one of the cast function.
   * @param funcName
   * @return
   */
  public static boolean isCastFunction(String funcName) {
    return TYPE_TO_CAST_FUNC.values().contains(funcName);
  }

  private static String getReplacingFunctionFromNonNullable(String originalCastFunction, MinorType inputType) {
    if (inputType == MinorType.VARCHAR
        && FUNC_REPLACEMENT_FROM_NON_NULLABLE_VARCHAR.containsKey(originalCastFunction)) {
      return FUNC_REPLACEMENT_FROM_NON_NULLABLE_VARCHAR.get(originalCastFunction);
    }
    if (inputType == MinorType.VAR16CHAR
        && FUNC_REPLACEMENT_FROM_NON_NULLABLE_VAR16CHAR.containsKey(originalCastFunction)) {
      return FUNC_REPLACEMENT_FROM_NON_NULLABLE_VAR16CHAR.get(originalCastFunction);
    }
    if (inputType == MinorType.VARBINARY
        && FUNC_REPLACEMENT_FROM_NON_NULLABLE_VARBINARY.containsKey(originalCastFunction)) {
      return FUNC_REPLACEMENT_FROM_NON_NULLABLE_VARBINARY.get(originalCastFunction);
    }

    throw new DrillRuntimeException(
      String.format("replacing cast function for %s is not defined", originalCastFunction));
  }

  private static String getReplacingFunctionFromNullable(String originalCastFunction, MinorType inputType) {
    if (inputType == MinorType.VARCHAR
        && FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.containsKey(originalCastFunction)) {
      return FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.get(originalCastFunction);
    }
    if (inputType == MinorType.VAR16CHAR
        && FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.containsKey(originalCastFunction)) {
      return FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.get(originalCastFunction);
    }
    if (inputType == MinorType.VARBINARY
        && FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.containsKey(originalCastFunction)) {
      return FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.get(originalCastFunction);
    }

    throw new DrillRuntimeException(
      String.format("replacing cast function for %s is not defined", originalCastFunction));
  }
}
