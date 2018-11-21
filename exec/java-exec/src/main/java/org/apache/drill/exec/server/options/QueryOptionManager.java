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
package org.apache.drill.exec.server.options;

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.ExecConstants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link OptionManager} that holds options within {@link org.apache.drill.exec.ops.QueryContext}.
 */
public class QueryOptionManager extends InMemoryOptionManager {

  private static final Map<String, Object> defaultValues;
  static {
    Map<String, Object> values = new HashMap<>();
    values.put(ExecConstants.SQL_NODE_KIND, "");

    defaultValues = CaseInsensitiveMap.newImmutableMap(values);
  }

  public QueryOptionManager(OptionManager sessionOptions) {
    super(sessionOptions, createQueryOptionDefinitions(), CaseInsensitiveMap.newHashMap());
    setDefaultValues(definitions, defaults, defaultValues, OptionValue.OptionScope.QUERY);
  }

  @Override
  public OptionList getOptionList() {
    OptionList list = super.getOptionList();
    list.merge(fallback.getOptionList());
    return list;
  }

  public SessionOptionManager getSessionOptionManager() {
    return (SessionOptionManager) fallback;
  }

  public OptionManager getOptionManager(OptionValue.OptionScope scope) {
    switch (scope) {
      case SYSTEM:
        return getSessionOptionManager().getSystemOptionManager();
      case SESSION:
        return getSessionOptionManager();
      case QUERY:
        return this;
      case BOOT:
        throw new UnsupportedOperationException("There is no option manager for " + OptionValue.OptionScope.BOOT);
      default:
        throw new UnsupportedOperationException("Invalid type: " + scope);
    }
  }

  @Override
  protected OptionValue.OptionScope getScope() {
    return OptionValue.OptionScope.QUERY;
  }

  /**
   * Creates all the OptionDefinitions to be registered with the {@link QueryOptionManager}.
   *
   * @return a {@code CaseInsensitiveMap} containing option definitions for
   * {@link OptionValue.AccessibleScopes#QUERY} scope
   */
  private static Map<String, OptionDefinition> createQueryOptionDefinitions() {
    final OptionDefinition[] definitions = new OptionDefinition[] {
        new OptionDefinition(ExecConstants.SQL_NODE_KIND_VALIDATOR,
            new OptionMetaData(OptionValue.AccessibleScopes.QUERY, false, true)),
    };

    return Arrays.stream(definitions)
        .collect(Collectors.toMap(
            d -> d.getValidator().getOptionName(),
            Function.identity(),
            (o, n) -> n,
            CaseInsensitiveMap::newHashMap)
        );
  }
}
