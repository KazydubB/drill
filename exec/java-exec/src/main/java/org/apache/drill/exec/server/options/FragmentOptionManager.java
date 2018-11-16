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

import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.common.map.CaseInsensitiveMap;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link OptionManager} that holds options within {@link FragmentContextImpl}.
 */
public class FragmentOptionManager extends InMemoryOptionManager {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentOptionManager.class);

  private static final OptionValue.AccessibleScopes SCOPE = OptionValue.AccessibleScopes.QUERY;

  private CaseInsensitiveMap<OptionDefinition> definitions;
  private CaseInsensitiveMap<OptionValue> defaults = CaseInsensitiveMap.newHashMap();

  public FragmentOptionManager(OptionManager systemOptions, OptionList options) {
    super(systemOptions, getMapFromOptionList(options));
    definitions = createDefaultOptionDefinitions();
  }

  private static Map<String, OptionValue> getMapFromOptionList(final OptionList options) {
    final Map<String, OptionValue> tmp = Maps.newHashMap();
    for (final OptionValue value : options) {
      tmp.put(value.name, value);
    }
    return CaseInsensitiveMap.newImmutableMap(tmp);
  }

  @Override
  public void deleteAllLocalOptions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteLocalOption(String name) {
    throw new UnsupportedOperationException();
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

  @Override
  protected OptionValue.OptionScope getScope() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLocalOptionHelper(OptionValue value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OptionList getPublicOptionList() {
    Iterator<OptionValue> values = this.iterator(); // todo: do similarly as is done in this.iterator(); OR redefine local options
    OptionList optionList = new OptionList();

    while (values.hasNext()) {
      OptionValue value = values.next();

      if (!getOptionDefinition(value.getName()).getMetaData().isInternal()) {
        optionList.add(value);
      }
    }

    optionList.merge(super.getPublicOptionList());
    return optionList;
  }

  /**
   * Creates all the OptionDefinitions to be registered with the {@link SystemOptionManager}.
   * @return A map
   */
  private CaseInsensitiveMap<OptionDefinition> createDefaultOptionDefinitions() {
    final OptionDefinition[] definitions = new OptionDefinition[]{
        new OptionDefinition(new TypeValidators.StringValidator("sqlnode.kind", new OptionValidator.OptionDescription("Some description here!")),
            new OptionMetaData(SCOPE, false, false)),
    };
    populateDefaultValue(defaults, definitions[0], "");

    return Arrays.stream(definitions)
        .collect(Collectors.toMap(
            d -> d.getValidator().getOptionName(),
            Function.identity(),
            (o, n) -> n,
            CaseInsensitiveMap::newHashMap));
  }

  private static void populateDefaultValue(CaseInsensitiveMap<OptionValue> defaults, OptionDefinition definition, Object value) {
    // OptionValidator validator = definition.getValidator();
    // String name = validator.getOptionName();
    // defaults.put(name, defaultValue);

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
        optionValue = OptionValue.create(type, name, value, OptionValue.OptionScope.QUERY);
        break;
      default:
        throw new UnsupportedOperationException();
    }

    defaults.put(name, optionValue);
  }
}
