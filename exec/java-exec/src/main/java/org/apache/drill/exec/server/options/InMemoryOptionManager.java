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

import java.util.Map;

/**
 * This is an {@link OptionManager} that holds options in memory rather than in a persistent store. Options stored in
 * {@link SessionOptionManager}, {@link QueryOptionManager}, and {@link FragmentOptionManager} are held in memory
 * (see {@link #options}) whereas the {@link SystemOptionManager} stores options in a persistent store.
 */
public abstract class InMemoryOptionManager extends FallbackOptionManager {

  protected final Map<String, OptionDefinition> definitions;
  protected final Map<String, OptionValue> options;
  protected final Map<String, OptionValue> defaults;

  InMemoryOptionManager(final OptionManager fallback, final Map<String, OptionDefinition> definitions,
                        final Map<String, OptionValue> defaults) {
    super(fallback);
    this.definitions = definitions;
    this.defaults = defaults;
    options = CaseInsensitiveMap.newHashMap();
  }

  @Override
  OptionValue getLocalOption(final String name) {
    OptionValue value = options.get(name);
    if (value == null) {
      value = defaults.get(name);
    }
    return value;
  }

  @Override
  public void setLocalOptionHelper(final OptionValue value) {
    options.put(value.name, value);
  }

  @Override
  Iterable<OptionValue> getLocalOptions() {
    Map<String, OptionValue> localOptions = CaseInsensitiveMap.newHashMap();
    // populate the default options
    localOptions.putAll(defaults);
    // override if changed
    localOptions.putAll(options);
    return localOptions.values();
  }

  @Override
  public void deleteAllLocalOptions() {
    options.clear();
  }

  @Override
  public void deleteLocalOption(final String name) {
    options.remove(name);
  }

  @Override
  public OptionValue getDefault(String optionName) {
    OptionValue value = defaults.get(optionName);
    if (value == null) {
      value = fallback.getDefault(optionName);
    }
    return value;
  }

  @Override
  public OptionDefinition getOptionDefinition(String name) {
    OptionDefinition definition = definitions.get(name);
    if (definition == null) {
      definition = super.getOptionDefinition(name);
    }
    return definition;
  }

  static void setDefaultValues(Map<String, OptionDefinition> definitions,
                               Map<String, OptionValue> defaults,
                               Map<String, Object> defaultValues,
                               OptionValue.OptionScope scope) {
    for (Map.Entry<String, Object> entry : defaultValues.entrySet()) {
      OptionDefinition definition = definitions.get(entry.getKey());
      OptionMetaData metaData = definition.getMetaData();
      OptionValue.AccessibleScopes type = metaData.getAccessibleScopes();
      OptionValidator validator = definition.getValidator();
      String name = validator.getOptionName();
      OptionValue.Kind kind = validator.getKind();
      OptionValue optionValue;

      switch (kind) {
      case BOOLEAN:
      case LONG:
      case STRING:
      case DOUBLE:
        optionValue = OptionValue.create(type, name, entry.getValue(), scope);
        break;
      default:
        throw new IllegalStateException(
            String.format("Default value for %s-scoped option '%s' is not defined.", scope.name(), entry.getKey()));
      }

      defaults.put(entry.getKey(), optionValue);
    }
  }
}
