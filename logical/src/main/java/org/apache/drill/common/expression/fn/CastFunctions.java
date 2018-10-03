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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;

public class CastFunctions {

  private static Map<MinorType, String> TYPE2FUNC = new HashMap<>();
  /** The cast functions that need to be replaced (if
   * "drill.exec.functions.cast_empty_string_to_null" is set to true). */
  private static Set<String> CAST_FUNC_REPLACEMENT_NEEDED = new HashSet<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARCHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for non-nullable VAR16CHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARBINARY). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARCHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VAR16CHAR). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARBINARY). */
  private static Map<String, String> CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY = new HashMap<>();
  static {
    TYPE2FUNC.put(MinorType.UNION, "castUNION");
    TYPE2FUNC.put(MinorType.BIGINT, "castBIGINT");
    TYPE2FUNC.put(MinorType.INT, "castINT");
    TYPE2FUNC.put(MinorType.BIT, "castBIT");
    TYPE2FUNC.put(MinorType.TINYINT, "castTINYINT");
    TYPE2FUNC.put(MinorType.FLOAT4, "castFLOAT4");
    TYPE2FUNC.put(MinorType.FLOAT8, "castFLOAT8");
    TYPE2FUNC.put(MinorType.VARCHAR, "castVARCHAR");
    TYPE2FUNC.put(MinorType.VAR16CHAR, "castVAR16CHAR");
    TYPE2FUNC.put(MinorType.VARBINARY, "castVARBINARY");
    TYPE2FUNC.put(MinorType.DATE, "castDATE");
    TYPE2FUNC.put(MinorType.TIME, "castTIME");
    TYPE2FUNC.put(MinorType.TIMESTAMP, "castTIMESTAMP");
    TYPE2FUNC.put(MinorType.TIMESTAMPTZ, "castTIMESTAMPTZ");
    TYPE2FUNC.put(MinorType.INTERVALDAY, "castINTERVALDAY");
    TYPE2FUNC.put(MinorType.INTERVALYEAR, "castINTERVALYEAR");
    TYPE2FUNC.put(MinorType.INTERVAL, "castINTERVAL");
    TYPE2FUNC.put(MinorType.DECIMAL9, "castDECIMAL9");
    TYPE2FUNC.put(MinorType.DECIMAL18, "castDECIMAL18");
    TYPE2FUNC.put(MinorType.DECIMAL28SPARSE, "castDECIMAL28SPARSE");
    TYPE2FUNC.put(MinorType.DECIMAL28DENSE, "castDECIMAL28DENSE");
    TYPE2FUNC.put(MinorType.DECIMAL38SPARSE, "castDECIMAL38SPARSE");
    TYPE2FUNC.put(MinorType.DECIMAL38DENSE, "castDECIMAL38DENSE");
    TYPE2FUNC.put(MinorType.VARDECIMAL, "castVARDECIMAL");

    // Numeric types
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.INT));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.BIGINT));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.FLOAT4));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.FLOAT8));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL9));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL18));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL28SPARSE));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DECIMAL38SPARSE));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.VARDECIMAL));
    // Date types
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.DATE));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.TIME));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.TIMESTAMP));
    // Interval types
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.INTERVAL));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.INTERVALDAY));
    CAST_FUNC_REPLACEMENT_NEEDED.add(TYPE2FUNC.get(MinorType.INTERVALYEAR));

    setupReplacementFunctions(MinorType.INT, "NullableINT");
    setupReplacementFunctions(MinorType.BIGINT, "NullableBIGINT");
    setupReplacementFunctions(MinorType.FLOAT4, "NullableFLOAT4");
    setupReplacementFunctions(MinorType.FLOAT8, "NullableFLOAT8");
    setupReplacementFunctions(MinorType.DECIMAL9, "NullableDECIMAL9");
    setupReplacementFunctions(MinorType.DECIMAL18, "NullableDECIMAL18");
    setupReplacementFunctions(MinorType.DECIMAL28SPARSE, "NullableDECIMAL28SPARSE");
    setupReplacementFunctions(MinorType.DECIMAL38SPARSE, "NullableDECIMAL38SPARSE");
    setupReplacementFunctions(MinorType.VARDECIMAL, "NullableVARDECIMAL");

    setupReplacementFunctions(MinorType.DATE, "NULLABLEDATE");
    setupReplacementFunctions(MinorType.TIME, "NULLABLETIME");
    setupReplacementFunctions(MinorType.TIMESTAMP, "NULLABLETIMESTAMP");

    setupReplacementFunctions(MinorType.INTERVAL, "NullableINTERVAL");
    setupReplacementFunctions(MinorType.INTERVALDAY, "NullableINTERVALDAY");
    setupReplacementFunctions(MinorType.INTERVALYEAR, "NullableINTERVALYEAR");
  }

  private static void setupReplacementFunctions(MinorType type, String toType) {
    String functionName = TYPE2FUNC.get(type);

    CAST_FUNC_REPLACEMENT_NEEDED.add(functionName);

    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(functionName, "castEmptyStringVarCharTo" + toType);
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(functionName, "castEmptyStringVar16CharTo" + toType);
    CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(functionName, "castEmptyStringVarBinaryTo" + toType);

    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(functionName, "castEmptyStringNullableVarCharTo" + toType);
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(functionName, "castEmptyStringNullableVar16CharTo" + toType);
    CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(functionName, "castEmptyStringNullableVarBinaryTo" + toType);
  }

  /**
  * Given the target type, get the appropriate cast function
  * @param targetMinorType the target data type
  * @return the name of cast function
  */
  public static String getCastFunc(MinorType targetMinorType) {
    String func = TYPE2FUNC.get(targetMinorType);
    if (func != null) {
      return func;
    }

    throw new IllegalArgumentException(
      String.format("cast function for type %s is not defined", targetMinorType.name()));
  }

  /**
  * Get a replacing cast function for the original function, based on the specified data mode
  * @param originalCastFunction original cast function
  * @param dataMode data mode of the input data
  * @param inputType input (minor) type for cast
  * @return the name of replaced cast function
  */
  public static String getReplacingCastFunction(String originalCastFunction, DataMode dataMode, MinorType inputType) {
    if(dataMode == DataMode.OPTIONAL) {
      return getReplacingCastFunctionFromNullable(originalCastFunction, inputType);
    }

    if(dataMode == DataMode.REQUIRED) {
      return getReplacingCastFunctionFromNonNullable(originalCastFunction, inputType);
    }

    throw new RuntimeException(
       String.format("replacing cast function for datatype %s is not defined", dataMode));
  }

  /**
  * Check if a replacing cast function is available for the the original function
  * @param originalfunction original cast function
  * @param inputType input (minor) type for cast
  * @return true if replacement is needed, false - if isn't
  */
  public static boolean isReplacementNeeded(String originalfunction, MinorType inputType) {
    return (inputType == MinorType.VARCHAR || inputType == MinorType.VARBINARY || inputType == MinorType.VAR16CHAR) &&
        CAST_FUNC_REPLACEMENT_NEEDED.contains(originalfunction);
  }

  /**
   * Check if a funcName is one of the cast function.
   * @param funcName
   * @return
   */
  public static boolean isCastFunction(String funcName) {
    return TYPE2FUNC.values().contains(funcName);
  }

  private static String getReplacingCastFunctionFromNonNullable(String originalCastFunction, MinorType inputType) {
    if(inputType == MinorType.VARCHAR && CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.get(originalCastFunction);
    }
    if(inputType == MinorType.VAR16CHAR && CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.get(originalCastFunction);
    }
    if(inputType == MinorType.VARBINARY && CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.get(originalCastFunction);
    }

    throw new RuntimeException(
      String.format("replacing cast function for %s is not defined", originalCastFunction));
  }

  private static String getReplacingCastFunctionFromNullable(String originalCastFunction, MinorType inputType) {
    if(inputType == MinorType.VARCHAR && CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.get(originalCastFunction);
    }
    if(inputType == MinorType.VAR16CHAR && CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.get(originalCastFunction);
    }
    if(inputType == MinorType.VARBINARY && CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.containsKey(originalCastFunction)) {
      return CAST_FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.get(originalCastFunction);
    }

    throw new RuntimeException(
      String.format("replacing cast function for %s is not defined", originalCastFunction));
  }
}
