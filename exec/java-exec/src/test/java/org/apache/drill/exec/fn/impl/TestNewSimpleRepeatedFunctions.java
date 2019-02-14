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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

@Category(SqlFunctionTest.class)
public class TestNewSimpleRepeatedFunctions extends ClusterTest {

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testRepeatedContainsForWildCards() throws Exception {
    testBuilder()
        .sqlQuery("select repeated_contains(topping, 'Choc*') from cp.`testRepeatedWrite.json`")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValuesForSingleColumn(true, true, true, true, false)
        .go();

    testBuilder()
        .sqlQuery("select repeated_contains(topping, 'Pow*') from cp.`testRepeatedWrite.json`")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValuesForSingleColumn(true, false, false, true, false)
        .go();
  }

  @Test
  public void testRepeatedCountRepeatedMap() throws Exception {
    // Contents of the generated file:
    /*
      {"mapArray": [{"field1": 1, "field2": "val1"}, {"field1": 2}]}
      {"mapArray": [{"field1": 1}, {"field1": 2}]}
      {"mapArray": [{"field2": "val2"}, {"field1": 2}, {"field1": 2}]}
      {"mapArray": []}
      {"mapArray": [{"field1": 1, "field2": "val3"}]}
      {"mapArray": [{"field1": 1, "field2": "val4"}, {"field1": 2}, {"field1": 2}, {"field2": "val1"}, {"field1": 2}]}
      {"mapArray": [{"field1": 1, "field2": "val3"}, {"field1": 2}]}
    */
    String fileName = "repeated_count_map.json";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(
        new File(dirTestWatcher.getRootDir(), fileName)))) {
      String[] arrayElements = {
          "{\"field1\": 1, \"field2\": \"val1\"}, {\"field1\": 2}",
          "{\"field1\": 1}, {\"field1\": 2}",
          "{\"field2\": \"val2\"}, {\"field1\": 2}, {\"field1\": 2}",
          "",
          "{\"field1\": 1, \"field2\": \"val3\"}",
          "{\"field1\": 1, \"field2\": \"val4\"}, {\"field1\": 2}, {\"field1\": 2}, {\"field2\": \"val1\"}, {\"field1\": 2}",
          "{\"field1\": 1, \"field2\": \"val3\"}, {\"field1\": 2}"
      };
      for (String value : arrayElements) {
        String entry = String.format("{\"mapArray\": [%s]}\n", value);
        writer.write(entry);
      }
    }

    String selectQuery = "select repeated_count(mapArray) from dfs.`%s`";
    testBuilder()
        .sqlQuery(selectQuery, fileName)
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValuesForSingleColumn(2, 2, 3, 0, 1, 5, 2)
        .go();

    testBuilder()
        .sqlQuery(selectQuery + " where repeated_count(mapArray) > 2", fileName)
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValuesForSingleColumn(3, 5)
        .go();

    testBuilder()
        .sqlQuery(selectQuery + " group by 1", fileName)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValuesForSingleColumn(2, 3, 0, 1, 5)
        .go();

    testBuilder()
        .sqlQuery(selectQuery + " group by 1 having repeated_count(mapArray) < 3", fileName)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValuesForSingleColumn(2, 0, 1)
        .go();
  }

  @Test
  public void testRepeatedCountRepeatedList() throws Exception {
    // Contents of the generated file:
    /*
      {"id": 1, "array": [[1, 2], [1, 3], [2, 3]]}
      {"id": 2, "array": []}
      {"id": 3, "array": [[2, 3], [1, 3, 4]]}
      {"id": 4, "array": [[1], [2], [3, 4], [5], [6]]}
      {"id": 5, "array": [[1, 2, 3], [4, 5], [6], [7], [8, 9], [2, 3], [2, 3], [2, 3], [2]]}
      {"id": 6, "array": [[1, 2], [3], [4], [5]]}
      {"id": 7, "array": []}
      {"id": 8, "array": [[1], [2], [3]]}
    */
    String fileName = "repeated_count_list.json";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(
        new File(dirTestWatcher.getRootDir(), fileName)))) {
      String[] arrayElements = {
          "[1, 2], [1, 3], [2, 3]",
          "",
          "[2, 3], [1, 3, 4]",
          "[1], [2], [3, 4], [5], [6]",
          "[1, 2, 3], [4, 5], [6], [7], [8, 9], [2, 3], [2, 3], [2, 3], [2]",
          "[1, 2], [3], [4], [5]",
          "",
          "[1], [2], [3]"};
      int elementId = 1;
      for (String value : arrayElements) {
        String entry = String.format("{\"id\": %d, \"array\": [%s]}\n", elementId++, value);
        writer.write(entry);
      }
    }

    String selectQuery = "select repeated_count(array) from dfs.`%s`";
    testBuilder()
        .sqlQuery(selectQuery, fileName)
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValuesForSingleColumn(3, 0, 2, 5, 9, 4, 0, 3)
        .go();

    testBuilder()
        .sqlQuery(selectQuery + " where repeated_count(array) > 4", fileName)
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValuesForSingleColumn(5, 9)
        .go();

    testBuilder()
        .sqlQuery(selectQuery + " group by 1", fileName)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValuesForSingleColumn(3, 0, 2, 5, 9, 4)
        .go();

    testBuilder()
        .sqlQuery(selectQuery + " group by 1 having repeated_count(array) < 4", fileName)
        .unOrdered()
        .baselineColumns("EXPR$0")
        .baselineValuesForSingleColumn(3, 0, 2)
        .go();
  }
}