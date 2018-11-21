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

import java.util.Map;

/**
 * {@link OptionManager} that holds options within {@link org.apache.drill.exec.ops.FragmentContext}.
 */
public class FragmentOptionManager extends InMemoryOptionManager {

  public FragmentOptionManager(OptionManager fallbackOptions, OptionList options) {
    super(fallbackOptions, CaseInsensitiveMap.newHashMap(), getMapFromOptionList(options));
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
  protected OptionValue.OptionScope getScope() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLocalOptionHelper(OptionValue value) {
    throw new UnsupportedOperationException();
  }
}
