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
package org.apache.drill;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.categories.SqlTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;

@Category({SqlTest.class, OperatorTest.class})
public class TestExcept extends ClusterTest {

  private static final String sliceTargetSmall = "alter session set `planner.slice_target` = 1";
  private static final String sliceTargetDefault = "alter session reset `planner.slice_target`";
  private static final String enableDistribute = "alter session set `planner.enable_unionall_distribute` = true";
  private static final String defaultDistribute = "alter session reset `planner.enable_unionall_distribute`";

  private static final String EMPTY_DIR_NAME = "empty_directory";

  @BeforeClass
  public static void setupTestFiles() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("json", "except"));
    dirTestWatcher.makeTestTmpSubDir(Paths.get(EMPTY_DIR_NAME));

    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testExcept() throws Exception {
//    String query = "explain plan for ((select n_regionkey from cp.`tpch/nation.parquet`) except (select n_regionkey from cp.`tpch/nation.parquet`))";
    String query = "select a from cp.`json/except/right.json` except select a from cp.`json/except/left.json`"; // todo: uncomment for all
//    String query = "(select a from cp.`json/except/left.json`) except (select a from cp.`json/except/right.json`)";
//    String query = "(select a from cp.`json/except/left.json`) union all (select a from cp.`json/except/right.json`)";
//    String query = "select a from ((select a from cp.`json/except/left.json`) union all (select a from cp.`json/except/right.json`)) group by a";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        //.csvBaselineFile("testframework/testUnionAllQueries/q1.tsv")
        //.baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("a")
//        .baselineColumns("json", "text")
//        .baselineColumns("json", "text")
//        .baselineValuesForSingleColumn(1)
//        .expectsEmptyResultSet()
//        .baselineValuesForSingleColumn(7L) // todo: this is it!
        .baselineValuesForSingleColumn(7L)
//        .baselineValues("wgfawa", "fagw")
        .go();
  }

  @Test
  public void testExceptWithCount() throws Exception {
//    String query = "explain plan for ((select n_regionkey from cp.`tpch/nation.parquet`) except (select n_regionkey from cp.`tpch/nation.parquet`))";
    String query = "select a, count(b) from cp.`json/except/right.json` group by a except select a, count(b) from cp.`json/except/left.json` group by 1"; // todo: uncomment for all
//    String query = "(select a from cp.`json/except/left.json`) except (select a from cp.`json/except/right.json`)";
//    String query = "(select a from cp.`json/except/left.json`) union all (select a from cp.`json/except/right.json`)";
//    String query = "select a from ((select a from cp.`json/except/left.json`) union all (select a from cp.`json/except/right.json`)) group by a";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        //.csvBaselineFile("testframework/testUnionAllQueries/q1.tsv")
        //.baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("a", "EXPR$1")
//        .baselineColumns("json", "text")
//        .baselineColumns("json", "text")
//        .baselineValuesForSingleColumn(1)
//        .expectsEmptyResultSet()
        .baselineValues(1L, 1L)
        .baselineValues(2L, 3L)
        .baselineValues(4L, 4L)
        .baselineValues(6L, 3L)
        .baselineValues(7L, 1L)
        .baselineValues(8L, 2L)
//        .baselineValues("wgfawa", "fagw")
        .go();
  }

  @Test
  public void testExcept2Fields() throws Exception {
    String query = "select a, count(b) as cnt from cp.`json/except/right.json` group by a except select a, count(b) as cnt from cp.`json/except/left.json` group by a";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("a", "cnt")
        .baselineValues(1L, 1L)
        .baselineValues(2L, 3L)
        .baselineValues(4L, 4L)
        .baselineValues(6L, 3L)
        .baselineValues(7L, 1L)
        .baselineValues(8L, 2L)
        .go();
  }

  @Test
  public void testExcept2Fields2() throws Exception {
//    String query = "select b, a from cp.`json/except/right.json` except all select b, a from cp.`json/except/left.json`";
    String query = "select b, a from cp.`json/except/right.json` except select b, a from cp.`json/except/left.json`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("b", "a")
        .baselineValues("e", 4L)
        .baselineValues("f", 4L)
        .baselineValues("g", 2L)
        .baselineValues("h", 2L)
        .baselineValues("i", 6L)
        .baselineValues("j", 6L)
        .baselineValues("k", 6L)
        .baselineValues("l", 7L)
        .baselineValues("m", 8L)
        .baselineValues("n", 8L)
//        .baselineValues("g", 2L)
//        .baselineValues("h", 2L)
//        .baselineValues("e", 4L)
//        .baselineValues("f", 4L)
//        .baselineValues("i", 6L)
//        .baselineValues("j", 6L)
//        .baselineValues("k", 6L)
//        .baselineValues("l", 7L)
//        .baselineValues("m", 8L)
//        .baselineValues("n", 8L)
        .go();
  }

  @Test
  public void testExceptAll2Fields2() throws Exception {
    String query = "select b, a from cp.`json/except/right.json` except all select b, a from cp.`json/except/left.json`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("b", "a")
        .baselineValues("e", 4L)
        .baselineValues("f", 4L)
        .baselineValues("f", 4L)
        .baselineValues("g", 2L)
        .baselineValues("h", 2L)
        .baselineValues("i", 6L)
        .baselineValues("j", 6L)
        .baselineValues("k", 6L)
        .baselineValues("l", 7L)
        .baselineValues("m", 8L)
        .baselineValues("n", 8L)
        .go();
  }

  @Test
  public void testExceptAll() throws Exception {
//    String query = "explain plan for ((select n_regionkey from cp.`tpch/nation.parquet`) except (select n_regionkey from cp.`tpch/nation.parquet`))";
    String query = "(select a from cp.`json/except/right.json`) except all (select a from cp.`json/except/left.json`)"; // todo: uncomment for all
//    String query = "(select a from cp.`json/except/left.json`) except (select a from cp.`json/except/right.json`)";
//    String query = "(select a from cp.`json/except/left.json`) union all (select a from cp.`json/except/right.json`)";
//    String query = "select a from ((select a from cp.`json/except/left.json`) union all (select a from cp.`json/except/right.json`)) group by a";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        //.csvBaselineFile("testframework/testUnionAllQueries/q1.tsv")
        //.baselineTypes(TypeProtos.MinorType.INT)
        .baselineColumns("a")
//        .baselineColumns("json", "text")
//        .baselineColumns("json", "text")
//        .baselineValuesForSingleColumn(1)
//        .expectsEmptyResultSet()
        .baselineValuesForSingleColumn(2L, 4L, 4L, 4L, 6L, 6L, 7L, 8L)
//        .baselineValues("wgfawa", "fagw")
        .go();
  }

//  @Test
//  public void testIntersect() throws Exception { // todo: remove!
//    String query = "(select a from cp.`json/except/right.json`) except (select a from cp.`json/except/left.json`)";
//
//    testBuilder()
//        .sqlQuery(query)
//        .unOrdered()
//        .baselineColumns("a")
//        .baselineValuesForSingleColumn(1L, 2L, 3L, 4L, 6L, 8L)
//        .go();
//  }
//
//  @Test
//  public void testIntersectAll() throws Exception {
//    String query = "(select a from cp.`json/except/left.json`) except all (select a from cp.`json/except/right.json`)";
//
//    testBuilder()
//        .sqlQuery(query)
//        .unOrdered()
//        .baselineColumns("a")
//        .baselineValuesForSingleColumn(1L, 2L, 2L, 3L, 4L, 6L, 8L)
//        .go();
//  }

  @Test
  public void testAggregate() throws Exception {
    String query = "select a from cp.`json/except/right.json` group by a";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("a")
        .baselineValues(1L)
        .baselineValues(2L)
        .baselineValues(3L)
        .baselineValues(4L)
        .baselineValues(6L)
        .baselineValues(7L)
        .baselineValues(8L)
        .go();
  }
}
