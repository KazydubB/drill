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

public class ToFunctions {

  private static Map<MinorType, String> TO_FUNC = new HashMap<>();
  /** The to functions that need to be replaced (if
   * "drill.exec.functions.cast_empty_string_to_null" is set to true). */
  private static Set<String> TO_FUNC_REPLACEMENT_NEEDED = new HashSet<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARCHAR). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for non-nullable VAR16CHAR). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for non-nullable VARBINARY). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARCHAR). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VAR16CHAR). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR = new HashMap<>();
  /** Map from the replaced functions to the new ones (for nullable VARBINARY). */
  private static Map<String, String> FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY = new HashMap<>();

  static {
    TO_FUNC.put(MinorType.DATE, "to_date");
    TO_FUNC.put(MinorType.TIME, "to_time");
    TO_FUNC.put(MinorType.TIMESTAMP, "to_timestamp");

    setupReplacementFunctions(MinorType.DATE, "NullableDate");
    setupReplacementFunctions(MinorType.TIME, "NullableTime");
    setupReplacementFunctions(MinorType.TIMESTAMP, "NullableTimeStamp");
  }

  private static void setupReplacementFunctions(MinorType type, String toType) {
    String functionName = TO_FUNC.get(type);

    TO_FUNC_REPLACEMENT_NEEDED.add(functionName);

    FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.put(functionName, "convertVarCharTo" + toType);
    FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.put(functionName, "convertVar16CharTo" + toType);
    FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.put(functionName, "convertVarBinaryTo" + toType);
    FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.put(functionName, "convertNullableVarCharTo" + toType);
    FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.put(functionName, "convertNullableVar16CharTo" + toType);
    FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.put(functionName, "convertNullableVarBinaryTo" + toType);
  }

  /**
   * Get a replacing to_ function for the original function, based on the specified data mode. Should be used
   * iff {@link #isReplacementNeeded(String, MinorType)} returns {@code true}.
   *
   * @param functionName original function name
   * @param dataMode data mode of the input data
   * @param inputType input (minor) type
   * @return the name of replaced function
   */
  public static String getReplacingFunction(String functionName, DataMode dataMode, MinorType inputType) {
    if(dataMode == DataMode.OPTIONAL) {
      return getReplacingFunctionFromNullable(functionName, inputType);
    }

    if(dataMode == DataMode.REQUIRED) {
      return getReplacingFunctionFromNonNullable(functionName, inputType);
    }

    throw new RuntimeException(String.format("replacing to function for data mode %s is not defined", dataMode));
  }

  /**
   * Check if a replacing to function is available for the the original function
   * @param functionName original to function
   * @param inputType input (minor) type for to
   * @return {@code true} if replacement is needed, {@code false} otherwise
   */
  public static boolean isReplacementNeeded(String functionName, MinorType inputType) {
    return (inputType == MinorType.VARCHAR || inputType == MinorType.VARBINARY || inputType == MinorType.VAR16CHAR)
        && TO_FUNC_REPLACEMENT_NEEDED.contains(functionName);
  }

  private static String getReplacingFunctionFromNonNullable(String functionName, MinorType inputType) {
    if(inputType == MinorType.VARCHAR && FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.containsKey(functionName)) {
      return FUNC_REPLACEMENT_FROM_NONNULLABLE_VARCHAR.get(functionName);
    }
    if(inputType == MinorType.VAR16CHAR && FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.containsKey(functionName)) {
      return FUNC_REPLACEMENT_FROM_NONNULLABLE_VAR16CHAR.get(functionName);
    }
    if(inputType == MinorType.VARBINARY && FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.containsKey(functionName)) {
      return FUNC_REPLACEMENT_FROM_NONNULLABLE_VARBINARY.get(functionName);
    }


    throw new RuntimeException(String.format("replacing to_ function for %s is not defined", functionName));
  }

  private static String getReplacingFunctionFromNullable(String functionName, MinorType inputType) {
    if(inputType == MinorType.VARCHAR && FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.containsKey(functionName)) {
      return FUNC_REPLACEMENT_FROM_NULLABLE_VARCHAR.get(functionName);
    }
    if(inputType == MinorType.VAR16CHAR && FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.containsKey(functionName)) {
      return FUNC_REPLACEMENT_FROM_NULLABLE_VAR16CHAR.get(functionName);
    }
    if(inputType == MinorType.VARBINARY && FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.containsKey(functionName)) {
      return FUNC_REPLACEMENT_FROM_NULLABLE_VARBINARY.get(functionName);
    }

    throw new RuntimeException(String.format("replacing to_ function for %s is not defined", functionName));
  }
}
