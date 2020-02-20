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
    String query = "(select a from cp.`json/except/left.json`) except all (select a from cp.`json/except/right.json`)"; // todo: uncomment for all
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
        .baselineValuesForSingleColumn(1, 2)
//        .baselineValues("wgfawa", "fagw")
        .go();
  }
}
