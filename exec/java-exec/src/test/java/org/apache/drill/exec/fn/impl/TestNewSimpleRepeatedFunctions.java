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
package org.apache.drill.exec.fn.impl;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;

@Category(SqlFunctionTest.class)
public class TestNewSimpleRepeatedFunctions extends ClusterTest {

  private static final String SELECT_REPEATED_CONTAINS = "select repeated_contains(topping, '%s*') from cp.`testRepeatedWrite.json`";
  private static final String SELECT_REPEATED_COUNT_LIST = "select repeated_count(array) from dfs.`functions/repeated/repeated_list.json`";
  private static final String SELECT_REPEATED_COUNT_MAP = "select repeated_count(mapArray) from dfs.`functions/repeated/repeated_map.json`";
  private static final String COLUMN_NAME = "EXPR$0";

  @BeforeClass
  public static void setUp() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("functions", "repeated"));
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testRepeatedContainsForWildCards() throws Exception {
    performTest(SELECT_REPEATED_CONTAINS, new String[] {"Choc"}, true, true, true, true, false);
    performTest(SELECT_REPEATED_CONTAINS, new String[] {"Pow"}, true, false, false, true, false);
  }

  @Test
  public void testRepeatedCountRepeatedMap() throws Exception {
    performTest(SELECT_REPEATED_COUNT_MAP, 2, 2, 3, 0, 1, 5, 2);
  }

  @Test
  public void testRepeatedCountRepeatedMapInWhere() throws Exception {
    String query = SELECT_REPEATED_COUNT_MAP + " where repeated_count(mapArray) > 2";
    performTest(query, 3, 5);
  }

  @Test
  public void testRepeatedCountRepeatedMapInHaving() throws Exception {
    String query = SELECT_REPEATED_COUNT_MAP + " group by 1 having repeated_count(mapArray) < 3";
    performTest(query, 2, 0, 1);
  }

  @Test
  public void testRepeatedCountRepeatedList() throws Exception {
    performTest(SELECT_REPEATED_COUNT_LIST, 3, 0, 2, 5, 9, 4, 0, 3);
  }

  @Test
  public void testRepeatedCountRepeatedListInWhere() throws Exception {
    String query = SELECT_REPEATED_COUNT_LIST + " where repeated_count(array) > 4";
    performTest(query, 5, 9);
  }

  @Test
  public void testRepeatedCountRepeatedListInHaving() throws Exception {
    String query = SELECT_REPEATED_COUNT_LIST + " group by 1 having repeated_count(array) < 4";
    performTest(query, 3, 0, 2);
  }

  private void performTest(String query, Object... expectedValues) throws Exception {
    performTest(query, new Object[0], expectedValues);
  }

  private void performTest(String query, Object[] replacements, Object... expectedValues) throws Exception {
    testBuilder()
        .sqlQuery(query, replacements)
        .unOrdered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(expectedValues)
        .go();
  }
}
